package storage

import (
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

	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/s3fs"
)

const (
	tagDelimiter = "--"
)

// Driver represents a S3FS driver implementation of StorageDriver
type driver struct {
	config   gofig.Config
	awsCreds *credentials.Credentials
}

func init() {
	registry.RegisterStorageDriver(s3fs.Name, newDriver)
}

func newDriver() types.StorageDriver {
	return &driver{}
}

// Name returns the name of the driver
func (d *driver) Name() string {
	return s3fs.Name
}

// Init initializes the driver.
func (d *driver) Init(ctx types.Context, config gofig.Config) error {
	d.config = config

	fields := log.Fields{
		"accessKey": d.accessKey(),
		"secretKey": d.secretKey(),
		"region":    d.region(),
		"tag":       d.tag(),
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

	d.awsCreds = credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.StaticProvider{Value: credentials.Value{AccessKeyID: d.accessKey(), SecretAccessKey: d.secretKey()}},
			&credentials.EnvProvider{},
			&credentials.SharedCredentialsProvider{},
			&ec2rolecreds.EC2RoleProvider{
				Client: ec2metadata.New(session.New()),
			},
		})

	ctx.WithFields(fields).Info("storage driver initialized")
	return nil
}

// InstanceInspect returns an instance.
func (d *driver) InstanceInspect(
	ctx types.Context,
	opts types.Store) (*types.Instance, error) {

	return nil, nil
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
		// Only volumes with partition prefix
		if !strings.HasPrefix(*fileSystem.Name, d.tag()+tagDelimiter) {
			continue
		}

		volumeSD := &types.Volume{
			Name: d.getPrintableName(*fileSystem.Name),
			ID:   *fileSystem.Name,
			//Size:        *fileSystem.SizeInBytes.Value,
			Attachments: nil,
		}

		var atts []*types.VolumeAttachment
		if opts.Attachments {
			atts, err = d.getVolumeAttachments(ctx, *fileSystem.Name)
			if err != nil {
				return nil, err
			}
		}
		if len(atts) > 0 {
			volumeSD.Attachments = atts
		}
		volumesSD = append(volumesSD, volumeSD)
	}

	return volumesSD, nil
}

// VolumeInspect inspects a single volume.
func (d *driver) VolumeInspect(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeInspectOpts) (*types.Volume, error) {

	fileSystemName := volumeID

	_, err := d.s3Client().HeadBucket(
		&awss3.HeadBucketInput{
			Bucket: aws.String(fileSystemName),
		})

	if err != nil {
		return nil, err
	}

	volume := &types.Volume{
		Name: d.getPrintableName(fileSystemName),
		ID:   d.getPrintableName(fileSystemName),
		//Size:        *fileSystem.SizeInBytes.Value,
		Attachments: nil,
	}

	var atts []*types.VolumeAttachment

	if opts.Attachments {
		atts, err = d.getVolumeAttachments(ctx, fileSystemName)
		if err != nil {
			return nil, err
		}
	}
	if len(atts) > 0 {
		volume.Attachments = atts
	}
	return volume, nil
}

// VolumeCreate creates a new volume.
func (d *driver) VolumeCreate(
	ctx types.Context,
	name string,
	opts *types.VolumeCreateOpts) (*types.Volume, error) {

	_, err := d.s3Client().CreateBucket(
		&awss3.CreateBucketInput{
			Bucket: aws.String(name),
		})
	if err != nil {
		return nil, err
	}

	err = d.s3Client().WaitUntilBucketExists(
		&awss3.HeadBucketInput{
			Bucket: aws.String(name),
		})
	if err != nil {
		return nil, err
	}

	return d.VolumeInspect(ctx, name,
		&types.VolumeInspectOpts{Attachments: false})
}

// VolumeRemove removes a volume.
func (d *driver) VolumeRemove(
	ctx types.Context,
	volumeID string,
	opts types.Store) error {

	// Remove FileSystem
	_, err := d.s3Client().DeleteBucket(
		&awss3.DeleteBucketInput{
			Bucket: aws.String(volumeID),
		})
	if err != nil {
		return err
	}

	err = d.s3Client().WaitUntilBucketNotExists(
		&awss3.HeadBucketInput{
			Bucket: aws.String(volumeID),
		})
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

	return nil, "", nil
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

// Retrieve all filesystems with tags from AWS API. This is very expensive
// operation as it issues AWS SDK call per filesystem to retrieve tags.
func (d *driver) getAllFileSystems() ([]*awss3.Bucket, error) {
	resp, err := d.s3Client().ListBuckets(&awss3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	return resp.Buckets, nil
}

func (d *driver) getVolumeAttachments(ctx types.Context, volumeID string) (
	[]*types.VolumeAttachment, error) {

	if volumeID == "" {
		return nil, goof.New("missing volume ID")
	}
	return nil, nil
}

func (d *driver) getPrintableName(name string) string {
	return strings.TrimPrefix(name, d.tag()+tagDelimiter)
}

func (d *driver) getFullVolumeName(name string) string {
	return d.tag() + tagDelimiter + name
}

func (d *driver) s3Client() *awss3.S3 {
	config := aws.NewConfig().
		WithCredentials(d.awsCreds).
		WithRegion(d.region())

	if types.Debug {
		config = config.
			WithLogger(newAwsLogger()).
			WithLogLevel(aws.LogDebug)
	}

	return awss3.New(session.New(), config)
}

func (d *driver) accessKey() string {
	return d.config.GetString("s3fs.accessKey")
}

func (d *driver) secretKey() string {
	return d.config.GetString("s3fs.secretKey")
}

func (d *driver) region() string {
	return d.config.GetString("s3fs.region")
}

func (d *driver) tag() string {
	return d.config.GetString("s3fs.tag")
}

// Simple logrus adapter for AWS Logger interface
type awsLogger struct {
	logger *log.Logger
}

func newAwsLogger() *awsLogger {
	return &awsLogger{
		logger: log.StandardLogger(),
	}
}

func (l *awsLogger) Log(args ...interface{}) {
	l.logger.Println(args...)
}
