package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/akutz/gofig"
	"github.com/akutz/goof"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"

	"github.com/emccode/libstorage/api/context"
	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/objectivefs"
)

const (
	objectivefsBinary = "mount.objectivefs"
	objectivefsPrefix = "s3://"
)

var (
	ErrInvalidOutput  = goof.New("couldn't parse output from binary")
	ErrMissingVolID   = goof.New("missing volume ID")
	ErrMissingVolName = goof.New("missing volume name")
)

// Driver represents a OBJECTIVEFS driver implementation of StorageDriver
type driver struct {
	config gofig.Config
}

func init() {
	registry.RegisterStorageDriver(objectivefs.Name, newDriver)
}

func newDriver() types.StorageDriver {
	return &driver{}
}

// Name returns the name of the driver
func (d *driver) Name() string {
	return objectivefs.Name
}

// Init initializes the driver.
func (d *driver) Init(ctx types.Context, config gofig.Config) error {
	d.config = config

	fields := log.Fields{
		"accessKey":    d.accessKey(),
		"secretKey":    d.secretKey(),
		"region":       d.region(),
		"metadataHost": d.metadataHost(),
		"$PATH":        os.Getenv("PATH"),
	}

	if d.accessKey() == "" {
		fields["accessKey"] = ""
	} else {
		fields["accessKey"] = "******"
	}

	if d.secretKey() == "" {
		fields["secretKey"] = ""
	} else {
		fields["secretKey"] = "******"
	}

	if d.accessKey() == "" || d.secretKey() == "" {
		if d.metadataHost() == "" {
			return goof.WithFields(fields,
				"missing metadata host or credentials")
		}
	}

	if d.license() == "" {
		return goof.WithFields(fields,
			"missing objectivefs license")
	}

	if d.adminLicense() == "" {
		return goof.WithFields(fields,
			"missing objectivefs admin license")
	}

	if d.passphrase() == "" {
		return goof.WithFields(fields,
			"missing objectivefs passphrase")
	}

	// Make sure we have the objectivefsBinary available on this host
	if _, err := exec.LookPath(objectivefsBinary); err != nil {
		return goof.WithFields(fields,
			"missing objectivefs binary in path")
	}

	ctx.WithFields(fields).Info("storage driver initialized")
	return nil
}

// InstanceInspect returns an instance.
func (d *driver) InstanceInspect(
	ctx types.Context,
	opts types.Store) (*types.Instance, error) {

	// TODO(kasisnu): Get region as request metadata
	instanceID := &types.InstanceID{ID: d.region(), Driver: d.Name()}

	return &types.Instance{InstanceID: instanceID}, nil
}

// Type returns the type of storage a driver provides
func (d *driver) Type(ctx types.Context) (types.StorageType, error) {
	return types.Object, nil
}

// NextDeviceInfo returns the information about the driver's next available
// device workflow.
func (d *driver) NextDeviceInfo(
	ctx types.Context) (*types.NextDeviceInfo, error) {
	return nil, nil
}

// Volumes returns all volumes or a filtered list of volumes.
func (d *driver) Volumes(
	ctx types.Context,
	opts *types.VolumesOpts) ([]*types.Volume, error) {

	fileSystems, err := d.getAllFileSystems()
	if err != nil {
		return nil, err
	}

	var volumesSD []*types.Volume
	for _, fileSystem := range fileSystems {
		volumeSD := &types.Volume{
			Name: fileSystem.name,
			ID:   fileSystem.name,
			Type: string(types.Object),
			Fields: map[string]string{
				"region": fileSystem.region,
				"kind":   fileSystem.kind,
			},
			Attachments: nil,
		}

		var atts []*types.VolumeAttachment
		if opts.Attachments {
			atts, err = d.getVolumeAttachments(ctx, fileSystem.name)
			if err != nil {
				return nil, err
			}
		}
		volumeSD.Attachments = atts
		volumesSD = append(volumesSD, volumeSD)
	}

	return volumesSD, nil
}

// VolumeInspect inspects a single volume.
func (d *driver) VolumeInspect(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeInspectOpts) (*types.Volume, error) {

	if volumeID == "" {
		return nil, ErrMissingVolID
	}

	fileSystemName := volumeID

	volume := &types.Volume{
		Name:        fileSystemName,
		ID:          fileSystemName,
		Attachments: nil,
	}

	if opts.Attachments {
		var atts []*types.VolumeAttachment
		atts, err := d.getVolumeAttachments(ctx, fileSystemName)
		if err != nil {
			return nil, err
		}
		volume.Attachments = atts
	}
	return volume, nil
}

// VolumeCreate creates a new volume.
func (d *driver) VolumeCreate(
	ctx types.Context,
	name string,
	opts *types.VolumeCreateOpts) (*types.Volume, error) {

	if name == "" {
		return nil, ErrMissingVolName
	}

	cmd := d.objectivefsAdmin(
		"create",
		[]string{
			name,
		},
		map[string]string{
			"OBJECTIVEFS_PASSPHRASE": d.passphrase(),
		},
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	//TODO(kasisnu): Figure out how to do this with a basic(non-admin) license
	//               Write passphrase to stdin
	err := cmd.Run()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			return nil, goof.WithFields(log.Fields{
				"action": "volumeCreate",
				"args":   cmd.Args,
				"stdout": stdout.String(),
				"stderr": stderr.String(),
			}, "objectivefs create failed:"+exitError.Error())
		}
		return nil, err
	}

	if types.Debug {
		log.WithFields(
			log.Fields{
				"action": "volumeCreate",
				"args":   cmd.Args,
				"stdout": stdout.String(),
				"stderr": stderr.String(),
			},
		).Debug("objectivefs output")
	}

	return d.VolumeInspect(ctx, name,
		&types.VolumeInspectOpts{Attachments: false})
}

// VolumeRemove removes a volume.
func (d *driver) VolumeRemove(
	ctx types.Context,
	volumeID string,
	opts types.Store) error {

	// TODO(kasisnu): Add support for admin deletes, which don't require direct aws calls

	client := d.s3Client()

	deletables := []*awss3.ObjectIdentifier{}

	params := &awss3.ListObjectsInput{
		Bucket: aws.String(volumeID),
	}

	resp, err := client.ListObjects(params)
	if err != nil {
		return err
	}

	for _, key := range resp.Contents {
		deletables = append(deletables, &awss3.ObjectIdentifier{Key: key.Key})
	}

	for *resp.IsTruncated {
		resp, err := client.ListObjects(params)
		if err != nil {
			return err
		}

		for _, key := range resp.Contents {
			deletables = append(deletables, &awss3.ObjectIdentifier{Key: key.Key})
		}
	}

	//TODO(kasisnu): Batch delete requests for large buckets?
	if len(deletables) > 0 {
		_, err = client.DeleteObjects(&awss3.DeleteObjectsInput{
			Bucket: aws.String(volumeID),
			Delete: &awss3.Delete{
				Objects: deletables,
			},
		})
		if err != nil {
			return err
		}
	}

	_, err = client.DeleteBucket(
		&awss3.DeleteBucketInput{
			Bucket: aws.String(volumeID),
		},
	)
	if err != nil {
		return err
	}

	err = client.WaitUntilBucketNotExists(
		&awss3.HeadBucketInput{
			Bucket: aws.String(volumeID),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

// VolumeAttach attaches a volume and provides a token clients can use
// to validate that device has appeared locally.
func (d *driver) VolumeAttach(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeAttachOpts) (*types.Volume, string, error) {

	vol, err := d.VolumeInspect(ctx, volumeID,
		&types.VolumeInspectOpts{Attachments: true})
	if err != nil {
		return nil, "", err
	}

	return vol, "", nil
}

// VolumeDetach detaches a volume.
func (d *driver) VolumeDetach(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeDetachOpts) (*types.Volume, error) {

	return nil, nil
}

// VolumeCreateFromSnapshot (not implemented).
func (d *driver) VolumeCreateFromSnapshot(
	ctx types.Context,
	snapshotID, volumeName string,
	opts *types.VolumeCreateOpts) (*types.Volume, error) {
	return nil, types.ErrNotImplemented
}

// VolumeCopy copies an existing volume (not implemented)
func (d *driver) VolumeCopy(
	ctx types.Context,
	volumeID, volumeName string,
	opts types.Store) (*types.Volume, error) {
	return nil, types.ErrNotImplemented
}

// VolumeSnapshot snapshots a volume (not implemented)
func (d *driver) VolumeSnapshot(
	ctx types.Context,
	volumeID, snapshotName string,
	opts types.Store) (*types.Snapshot, error) {
	return nil, types.ErrNotImplemented
}

func (d *driver) Snapshots(
	ctx types.Context,
	opts types.Store) ([]*types.Snapshot, error) {
	return nil, nil
}

func (d *driver) SnapshotInspect(
	ctx types.Context,
	snapshotID string,
	opts types.Store) (*types.Snapshot, error) {
	return nil, nil
}

func (d *driver) SnapshotCopy(
	ctx types.Context,
	snapshotID, snapshotName, destinationID string,
	opts types.Store) (*types.Snapshot, error) {
	return nil, nil
}

func (d *driver) SnapshotRemove(
	ctx types.Context,
	snapshotID string,
	opts types.Store) error {

	return nil
}

func (d *driver) getVolumeAttachments(ctx types.Context, volumeID string) (
	[]*types.VolumeAttachment, error) {

	if volumeID == "" {
		return nil, ErrMissingVolID
	}

	var dev, status string
	dev = objectivefsPrefix + volumeID

	ld, ldOK := context.LocalDevices(ctx)
	if ldOK {
		if _, ok := ld.DeviceMap[dev]; ok {
			status = "Exported and Mounted"
		} else {
			status = "Exported and Unmounted"
		}
	} else {
		status = "Exported"
	}

	// There will only ever be one attachment
	attachments := []*types.VolumeAttachment{
		&types.VolumeAttachment{
			VolumeID:   volumeID,
			InstanceID: &types.InstanceID{ID: d.region(), Driver: d.Name()},
			DeviceName: dev,
			Status:     status,
		},
	}

	return attachments, nil
}

type filesystem struct {
	name, kind, region string
}

// getAllFileSystems parses to filesystem
//     NAME                                                                    KIND    REGION
//     s3://test-fs-12                                                         ofs     us-east-1
func (d *driver) getAllFileSystems() ([]filesystem, error) {
	cmd := d.objectivefs("list", nil, nil)
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var (
		scanner            = bufio.NewScanner(bytes.NewReader(output))
		filesystems        = []filesystem{}
		line               string
		fields             []string
		name, kind, region string
	)

	// Advance the scanner once to skip the header
	if !scanner.Scan() {
		// We have no output to parse
		// Not even a header
		return nil, ErrInvalidOutput
	}

	for scanner.Scan() {
		line = scanner.Text()
		fields = strings.Fields(line)
		if len(fields) != 3 {
			return nil, ErrInvalidOutput
		}
		name = strings.TrimPrefix(fields[0], objectivefsPrefix)
		kind = fields[1]
		region = fields[2]

		if name == "" || kind == "" || region == "" {
			return nil, ErrInvalidOutput
		}
		filesystems = append(filesystems,
			filesystem{
				name:   name,
				kind:   kind,
				region: region,
			})
	}

	return filesystems, nil
}

func (d *driver) accessKey() string {
	return d.config.GetString("objectivefs.accessKey")
}

func (d *driver) secretKey() string {
	return d.config.GetString("objectivefs.secretKey")
}

func (d *driver) region() string {
	return d.config.GetString("objectivefs.region")
}

func (d *driver) license() string {
	return d.config.GetString("objectivefs.license")
}

func (d *driver) adminLicense() string {
	return d.config.GetString("objectivefs.adminLicense")
}

func (d *driver) passphrase() string {
	return d.config.GetString("objectivefs.passphrase")
}

func (d *driver) metadataHost() string {
	return d.config.GetString("objectivefs.metadataHost")
}

func (d *driver) objectivefsDefaultEnv() []string {
	objectivefsEnv := []string{}
	objectivefsEnv = append(objectivefsEnv,
		fmt.Sprintf("%s=%s", "AWS_DEFAULT_REGION", d.region()),
	)

	if d.metadataHost() != "" {
		objectivefsEnv = append(objectivefsEnv,
			fmt.Sprintf("%s=%s", "AWS_METADATA_HOST", d.metadataHost()),
		)
	} else {
		objectivefsEnv = append(objectivefsEnv,
			fmt.Sprintf("%s=%s", "AWS_ACCESS_KEY_ID", d.accessKey()),
			fmt.Sprintf("%s=%s", "AWS_SECRET_ACCESS_KEY", d.secretKey()),
		)
	}
	return objectivefsEnv
}

func (d *driver) objectivefsEnv(env map[string]string) []string {
	objectivefsEnv := d.objectivefsDefaultEnv()
	for k, v := range env {
		objectivefsEnv = append(objectivefsEnv, fmt.Sprintf("%s=%s", k, v))
	}

	return objectivefsEnv
}

// Execute each command with a specific env
func (d *driver) objectivefs(subcommand string,
	args []string,
	env map[string]string,
) *exec.Cmd {

	objectivefsEnv := d.objectivefsEnv(env)
	objectivefsEnv = append(objectivefsEnv,
		fmt.Sprintf("%s=%s", "OBJECTIVEFS_LICENSE", d.license()),
	)

	args = append([]string{subcommand}, args...)
	cmd := exec.Command(objectivefsBinary, args...)
	cmd.Env = objectivefsEnv
	return cmd
}

// Execute each command with a specific env
func (d *driver) objectivefsAdmin(subcommand string,
	args []string,
	env map[string]string,
) *exec.Cmd {

	objectivefsEnv := d.objectivefsEnv(env)
	objectivefsEnv = append(objectivefsEnv,
		fmt.Sprintf("%s=%s", "OBJECTIVEFS_LICENSE", d.adminLicense()),
	)

	args = append([]string{subcommand}, args...)
	cmd := exec.Command(objectivefsBinary, args...)
	cmd.Env = objectivefsEnv
	return cmd
}

func (d *driver) s3Client() *awss3.S3 {
	client := awss3.New(session.New(),
		aws.NewConfig().
			WithRegion(d.region()).
			WithCredentials(credentials.NewChainCredentials(
				[]credentials.Provider{
					&credentials.StaticProvider{
						Value: credentials.Value{
							AccessKeyID:     d.accessKey(),
							SecretAccessKey: d.secretKey(),
						},
					},
					&credentials.EnvProvider{},
					&credentials.SharedCredentialsProvider{},
					&ec2rolecreds.EC2RoleProvider{
						Client: ec2metadata.New(session.New()),
					},
				})),
	)
	return client
}
