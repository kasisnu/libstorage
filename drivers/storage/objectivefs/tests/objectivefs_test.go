package objectivefs

// load the driver
import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/akutz/gofig"
	"github.com/emccode/libstorage/api/server"
	apitests "github.com/emccode/libstorage/api/tests"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/objectivefs"
	"github.com/stretchr/testify/assert"
)

var configYAML = []byte(`
objectivefs:
  license: %s
  adminLicense: %s
  region: %s
  metadataHost: %s
  accessKey: %s
  secretKey: %s
  passphrase: %s
`)

func skipTests() bool {
	travis, _ := strconv.ParseBool(os.Getenv("TRAVIS"))
	noTest, _ := strconv.ParseBool(os.Getenv("TEST_SKIP_OBJECTIVEFS"))
	return travis || noTest
}

var volumeName string
var volumeName2 string

func init() {
	if skipTests() {
		return
	}
	uuid, _ := types.NewUUID()
	uuids := strings.Split(uuid.String(), "-")
	volumeName = "objectivefs-test-" + uuids[0]
	uuid, _ = types.NewUUID()
	uuids = strings.Split(uuid.String(), "-")
	volumeName2 = "objectivefs-test-" + uuids[0]

	license := os.Getenv("OBJECTIVEFS_LICENSE")
	if license == "" {
		panic("OBJECTIVEFS_LICENSE cannot be empty")
	}
	adminLicense := os.Getenv("OBJECTIVEFS_ADMIN_LICENSE")
	if adminLicense == "" {
		panic("OBJECTIVEFS_ADMIN_LICENSE cannot be empty")
	}
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "us-east-1"
	}
	awsMetadataHost := os.Getenv("AWS_METADATA_HOST")
	if awsMetadataHost == "" {
		awsMetadataHost = "169.254.169.254"
	}
	passphrase := os.Getenv("OBJECTIVEFS_PASSPHRASE")
	if passphrase == "" {
		panic("OBJECTIVEFS_PASSPHRASE cannot be empty")
	}
	accessKey := ""
	secretKey := ""

	configYAML = []byte(
		fmt.Sprintf(string(configYAML[:]),
			license,
			adminLicense,
			awsRegion,
			awsMetadataHost,
			accessKey,
			secretKey,
			passphrase,
		),
	)

	log.WithFields(log.Fields{
		"config": string(configYAML),
	}).Info("Test environment initialized")
}

func TestMain(m *testing.M) {
	server.CloseOnAbort()
	ec := m.Run()
	os.Exit(ec)
}

func TestServices(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}

	tf := func(config gofig.Config, client types.Client, t *testing.T) {
		reply, err := client.API().Services(nil)
		assert.NoError(t, err)
		assert.Equal(t, len(reply), 1)

		_, ok := reply[objectivefs.Name]
		assert.True(t, ok)
	}
	apitests.Run(t, objectivefs.Name, configYAML, tf)
}

func volumeCreate(
	t *testing.T, client types.Client, volumeName string) *types.Volume {
	log.WithField("volumeName", volumeName).Info("creating volume")
	size := int64(1)

	opts := map[string]interface{}{
		"priority": 2,
		"owner":    "root@example.com",
	}

	volumeCreateRequest := &types.VolumeCreateRequest{
		Name: volumeName,
		Size: &size,
		Opts: opts,
	}

	reply, err := client.API().VolumeCreate(nil, objectivefs.Name, volumeCreateRequest)
	if err != nil {
		t.Error("failed volumeCreate", err)
		t.FailNow()
	}
	apitests.LogAsJSON(reply, t)

	assert.NotNil(t, reply)
	assert.Equal(t, volumeName, reply.Name)
	<-time.After(15 * time.Second)
	return reply
}

func volumeByName(
	t *testing.T, client types.Client, volumeName string) *types.Volume {

	log.WithField("volumeName", volumeName).Info("get volume name")
	vols, err := client.API().Volumes(nil, false)
	if err != nil {
		t.Error("failed volumeByName", err)
		t.FailNow()
	}
	assert.Contains(t, vols, objectivefs.Name)
	for _, vol := range vols[objectivefs.Name] {
		if vol.Name == volumeName {
			return vol
		}
	}
	t.FailNow()
	t.Error("failed volumeByName")
	return nil
}

func TestVolumeCreateRemove(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}

	tf := func(config gofig.Config, client types.Client, t *testing.T) {
		vol := volumeCreate(t, client, volumeName)
		volumeRemove(t, client, vol.ID)
	}
	apitests.Run(t, objectivefs.Name, configYAML, tf)
}

func volumeRemove(t *testing.T, client types.Client, volumeID string) {
	log.WithField("volumeID", volumeID).Info("removing volume")
	err := client.API().VolumeRemove(
		nil, objectivefs.Name, volumeID)
	if err != nil {
		t.Error("failed volumeRemove", err)
		t.FailNow()
	}
}

func TestVolumes(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}

	tf := func(config gofig.Config, client types.Client, t *testing.T) {
		_ = volumeCreate(t, client, volumeName)
		_ = volumeCreate(t, client, volumeName2)

		vol1 := volumeByName(t, client, volumeName)
		vol2 := volumeByName(t, client, volumeName2)

		volumeRemove(t, client, vol1.ID)
		volumeRemove(t, client, vol2.ID)
	}
	apitests.Run(t, objectivefs.Name, configYAML, tf)
}

func volumeAttach(
	t *testing.T, client types.Client, volumeID string) *types.Volume {

	log.WithField("volumeID", volumeID).Info("attaching volume")
	reply, token, err := client.API().VolumeAttach(
		nil, objectivefs.Name, volumeID, &types.VolumeAttachRequest{})

	if err != nil {
		t.Error("failed volumeAttach", err)
		t.FailNow()
	}
	apitests.LogAsJSON(reply, t)
	assert.Equal(t, token, "")

	return reply
}

func volumeInspectAttached(
	t *testing.T, client types.Client, volumeID string) *types.Volume {

	log.WithField("volumeID", volumeID).Info("inspecting volume")
	reply, err := client.API().VolumeInspect(nil, objectivefs.Name, volumeID, true)
	if err != nil {
		t.Error("failed volumeInspectAttached", err)
		t.FailNow()
	}
	apitests.LogAsJSON(reply, t)
	assert.Len(t, reply.Attachments, 1)
	assert.NotEqual(t, "", reply.Attachments[0].DeviceName)
	return reply
}

func volumeInspectDetached(
	t *testing.T, client types.Client, volumeID string) *types.Volume {

	log.WithField("volumeID", volumeID).Info("inspecting volume")
	reply, err := client.API().VolumeInspect(nil, objectivefs.Name, volumeID, true)
	if err != nil {
		t.Error("failed volumeInspectDetached", err)
		t.FailNow()
	}
	apitests.LogAsJSON(reply, t)
	assert.Len(t, reply.Attachments, 0)
	apitests.LogAsJSON(reply, t)
	return reply
}

func volumeDetach(
	t *testing.T, client types.Client, volumeID string) *types.Volume {

	log.WithField("volumeID", volumeID).Info("detaching volume")
	reply, err := client.API().VolumeDetach(
		nil, objectivefs.Name, volumeID, &types.VolumeDetachRequest{})
	if err != nil {
		t.Error("failed volumeDetach", err)
		t.FailNow()
	}
	apitests.LogAsJSON(reply, t)
	assert.Len(t, reply.Attachments, 0)
	return reply
}

func TestVolumeAttach(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}
	var vol *types.Volume
	tf := func(config gofig.Config, client types.Client, t *testing.T) {
		vol = volumeCreate(t, client, volumeName)
		_ = volumeAttach(t, client, vol.ID)
		_ = volumeInspectAttached(t, client, vol.ID)
		//Don't test detaching volumes
		//_ = volumeDetach(t, client, vol.ID)
		//_ = volumeInspectDetached(t, client, vol.ID)
		volumeRemove(t, client, vol.ID)
	}
	apitests.RunGroup(t, objectivefs.Name, configYAML, tf)
}
