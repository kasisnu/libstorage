package storage

// load the driver
import (
	"fmt"
	"os"
	"strconv"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/akutz/gofig"
	"github.com/emccode/libstorage/api/context"
	"github.com/emccode/libstorage/api/types"
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
`)

func skipTests() bool {
	travis, _ := strconv.ParseBool(os.Getenv("TRAVIS"))
	noTest, _ := strconv.ParseBool(os.Getenv("TEST_SKIP_OBJECTIVEFS"))
	return travis || noTest
}

var volumeName string
var volumeName2 string

func init() {
	volumeName, _ := types.NewUUID()
	volumeName2, _ := types.NewUUID()

	license := "that-key"
	adminLicense := "that-more-powerful-key"
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "us-east-1"
	}
	awsMetadataHost := "some-host"
	accessKey := ""
	secretKey := ""

	configYAML = []byte(fmt.Sprintf(string(configYAML[:]),
		license,
		adminLicense,
		awsRegion,
		awsMetadataHost,
		accessKey,
		secretKey,
	),
	)

	log.WithFields(log.Fields{
		"config": string(configYAML),
	}).Info("Test environment initialized")
	fmt.Println(volumeName, volumeName2)
}

func basicConfig() gofig.Config {
	r := gofig.New()
	r.Set("objectivefs.metadataHost", "some-host")
	r.Set("objectivefs.license", "some-license")
	r.Set("objectivefs.adminLicense", "some-admin-license")
	r.Set("objectivefs.region", "some-region")
	r.Set("objectivefs.passphrase", "some-passphrase")
	return r
}

func TestObjectivefs(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}

	sd := newDriver()
	ctx := context.Background()
	if err := sd.Init(ctx, basicConfig()); err != nil {
		t.Fatal(err)
	}
	d, _ := sd.(*driver)

	cmd := d.objectivefs("some-subcommand", nil, nil)
	assert.NotNil(t, cmd, "cmd cannot be nil")
	assert.NotNil(t, cmd.Args, "cmd args cannot be nil")
	assert.Equal(t, objectivefsBinary, cmd.Args[0], "not exec-ing objectivefs")
	assert.Equal(t, "some-subcommand", cmd.Args[1], "subcommand missing")
	assert.NotNil(t, cmd.Env, "cmd env cannot be nil")
}

func TestObjectivefsAdmin(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}

	sd := newDriver()
	ctx := context.Background()
	if err := sd.Init(ctx, basicConfig()); err != nil {
		t.Fatal(err)
	}
	d, _ := sd.(*driver)

	cmd := d.objectivefsAdmin("some-subcommand", nil, nil)
	assert.NotNil(t, cmd, "cmd cannot be nil")
	assert.NotNil(t, cmd.Args, "cmd args cannot be nil")
	assert.Equal(t, objectivefsBinary, cmd.Args[0], "not exec-ing objectivefs")
	assert.Equal(t, "some-subcommand", cmd.Args[1], "subcommand missing")
	assert.NotNil(t, cmd.Env, "cmd env cannot be nil")
	assert.Contains(t, cmd.Env, "OBJECTIVEFS_LICENSE=some-admin-license", "cmd env is missing an admin license")
}

func TestObjectivefsDefaultEnv(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}

	sd := newDriver()
	ctx := context.Background()
	if err := sd.Init(ctx, basicConfig()); err != nil {
		t.Fatal(err)
	}
	d, _ := sd.(*driver)
	env := d.objectivefsDefaultEnv()
	assert.Contains(t, env, "AWS_DEFAULT_REGION=some-region", "env is missing region")
	assert.Contains(t, env, "AWS_METADATA_HOST=some-host", "env is missing a metadata host")
}

func TestObjectivefsDefaultEnvWithKeys(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}

	sd := newDriver()
	ctx := context.Background()
	config := basicConfig()
	// Use keys instead of setting a metadata host
	config.Set("objectivefs.metadataHost", nil)
	config.Set("objectivefs.accessKey", "some-access-key")
	config.Set("objectivefs.secretKey", "some-secret-key")
	if err := sd.Init(ctx, config); err != nil {
		t.Fatal(err)
	}
	d, _ := sd.(*driver)
	env := d.objectivefsDefaultEnv()
	assert.Contains(t, env, "AWS_DEFAULT_REGION=some-region", "env is missing region")
	assert.Contains(t, env, "AWS_ACCESS_KEY_ID=some-access-key", "env is missing a access key")
	assert.Contains(t, env, "AWS_SECRET_ACCESS_KEY=some-secret-key", "env is missing a secret key")
}
