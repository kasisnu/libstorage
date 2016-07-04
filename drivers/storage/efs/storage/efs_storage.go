package storage

import (
	"errors"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/akutz/gofig"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	awsefs "github.com/aws/aws-sdk-go/service/efs"

	"github.com/emccode/libstorage/api/context"
	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/efs"
)

const (
	tagDelimiter = "/"
)

// Driver represents a EFS driver implementation of StorageDriver
type driver struct {
	config   gofig.Config
	awsCreds *credentials.Credentials
}

func init() {
	registry.RegisterStorageDriver(efs.Name, newDriver)
}

func newDriver() types.StorageDriver {
	return &driver{}
}

// Name returns the name of the driver
func (d *driver) Name() string {
	return efs.Name
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

	log.WithFields(fields).Info("storage driver initialized")
	return nil
}

// InstanceInspect returns an instance.
func (d *driver) InstanceInspect(
	ctx types.Context,
	opts types.Store) (*types.Instance, error) {

	iid := context.MustInstanceID(ctx)
	if iid.ID != "" {
		return &types.Instance{InstanceID: iid}, nil
	}

	var awsSubnetID string
	if err := iid.UnmarshalMetadata(&awsSubnetID); err != nil {
		return nil, err
	}
	instanceID := &types.InstanceID{ID: awsSubnetID, Driver: d.Name()}

	return &types.Instance{InstanceID: instanceID}, nil
}

// Type returns the type of storage a driver provides
func (d *driver) Type(ctx types.Context) (types.StorageType, error) {
	return types.NAS, nil
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
			Name:        d.getPrintableName(*fileSystem.Name),
			ID:          *fileSystem.FileSystemId,
			Size:        *fileSystem.SizeInBytes.Value,
			Attachments: nil,
		}

		var atts []*types.VolumeAttachment
		if opts.Attachments {
			atts, err = d.getVolumeAttachments(ctx, *fileSystem.FileSystemId)
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

	resp, err := d.efsClient().DescribeFileSystems(&awsefs.DescribeFileSystemsInput{
		FileSystemId: aws.String(volumeID),
	})
	if err != nil {
		return nil, err
	}
	if len(resp.FileSystems) > 0 {
		fileSystem := resp.FileSystems[0]
		volume := &types.Volume{
			Name:        d.getPrintableName(*fileSystem.Name),
			ID:          *fileSystem.FileSystemId,
			Size:        *fileSystem.SizeInBytes.Value,
			Attachments: nil,
		}

		var atts []*types.VolumeAttachment

		if opts.Attachments {
			atts, err = d.getVolumeAttachments(ctx, *fileSystem.FileSystemId)
			if err != nil {
				return nil, err
			}
		}
		if len(atts) > 0 {
			volume.Attachments = atts
		}
		return volume, nil
	}

	return nil, types.ErrNotFound{}
}

// VolumeCreate creates a new volume.
func (d *driver) VolumeCreate(
	ctx types.Context,
	name string,
	opts *types.VolumeCreateOpts) (*types.Volume, error) {

	fileSystem, err := d.efsClient().CreateFileSystem(&awsefs.CreateFileSystemInput{
		CreationToken: aws.String(name),
		// TODO(mhrabovcin): Add performance mode support
	})

	if err != nil {
		return nil, err
	}

	_, err = d.efsClient().CreateTags(&awsefs.CreateTagsInput{
		FileSystemId: fileSystem.FileSystemId,
		Tags: []*awsefs.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(d.getFullVolumeName(name)),
			},
		},
	})

	if err != nil {
		return nil, err
	}

	return d.VolumeInspect(ctx, *fileSystem.FileSystemId,
		&types.VolumeInspectOpts{Attachments: false})
}

// VolumeRemove removes a volume.
func (d *driver) VolumeRemove(
	ctx types.Context,
	volumeID string,
	opts types.Store) error {

	// Remove mountpoints
	mountTargets, err := d.efsClient().DescribeMountTargets(
		&awsefs.DescribeMountTargetsInput{
			FileSystemId: aws.String(volumeID),
		})
	if err != nil {
		return err
	}

	for _, mt := range mountTargets.MountTargets {
		_, err = d.efsClient().DeleteMountTarget(
			&awsefs.DeleteMountTargetInput{
				MountTargetId: aws.String(*mt.MountTargetId),
			})

		if err != nil {
			return err
		}
	}

	// Remove filesystem
	_, err = d.efsClient().DeleteFileSystem(
		&awsefs.DeleteFileSystemInput{
			FileSystemId: aws.String(volumeID),
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

	vol, err := d.VolumeInspect(ctx, volumeID,
		&types.VolumeInspectOpts{Attachments: true})
	if err != nil {
		return nil, "", err
	}

	inst, err := d.InstanceInspect(ctx, nil)
	if err != nil {
		return nil, "", err
	}

	var ma *types.VolumeAttachment
	for _, att := range vol.Attachments {
		if att.InstanceID.ID == inst.InstanceID.ID {
			ma = att
			break
		}
	}

	// No mount targets were found
	if ma == nil {
		_, err = d.efsClient().CreateMountTarget(
			&awsefs.CreateMountTargetInput{
				FileSystemId:   aws.String(vol.ID),
				SubnetId:       aws.String(inst.InstanceID.ID),
				SecurityGroups: aws.StringSlice(d.securityGroups()),
			})
		// Failed to create mount target
		if err != nil {
			return nil, "", err
		}
	}

	return vol, "", err
}

// VolumeDetach detaches a volume.
func (d *driver) VolumeDetach(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeDetachOpts) (*types.Volume, error) {

	// TODO(kasisnu): Think about what to do here?
	// It is safe to remove the mount target
	// when it is no longer being used anywhere
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
func (d *driver) getAllFileSystems() (filesystems []*awsefs.FileSystemDescription, err error) {
	resp, err := d.efsClient().DescribeFileSystems(&awsefs.DescribeFileSystemsInput{})
	if err != nil {
		return nil, err
	}
	filesystems = append(filesystems, resp.FileSystems...)

	for resp.NextMarker != nil {
		resp, err = d.efsClient().DescribeFileSystems(&awsefs.DescribeFileSystemsInput{
			Marker: resp.NextMarker,
		})
		if err != nil {
			return nil, err
		}
		filesystems = append(filesystems, resp.FileSystems...)
	}

	return filesystems, nil
}

func (d *driver) getPrintableName(name string) string {
	return strings.TrimLeft(name, d.tag()+tagDelimiter)
}

func (d *driver) getFullVolumeName(name string) string {
	return d.tag() + tagDelimiter + name
}

func (d *driver) getVolumeAttachments(ctx types.Context, volumeID string) (
	[]*types.VolumeAttachment, error) {

	if volumeID == "" {
		return nil, errors.New("Missing volume ID")
	}
	mountTargets, err := d.efsClient().DescribeMountTargets(
		&awsefs.DescribeMountTargetsInput{
			FileSystemId: aws.String(volumeID),
		})
	if err != nil {
		return nil, err
	}

	ld, ldOK := context.LocalDevices(ctx)

	var atts []*types.VolumeAttachment
	for _, target := range mountTargets.MountTargets {
		var dev string
		var status string
		if ldOK {
			//TODO(kasisnu): Check lifecycle state
			// and build the path better
			dev = *target.IpAddress + ":" + "/"
			if _, ok := ld.DeviceMap[dev]; ok {
				status = "Exported and Mounted"
			} else {
				status = "Exported and Unmounted"
			}
		} else {
			status = "Exported"
		}
		attachmentSD := &types.VolumeAttachment{
			VolumeID:   *target.FileSystemId,
			InstanceID: &types.InstanceID{ID: *target.SubnetId, Driver: d.Name()},
			DeviceName: dev,
			Status:     status,
		}
		atts = append(atts, attachmentSD)
	}

	return atts, nil
}

func (d *driver) efsClient() *awsefs.EFS {
	return awsefs.New(
		session.New(),
		aws.NewConfig().
			WithCredentials(d.awsCreds).
			WithRegion(d.region()))
	// Enable during dev
	//WithLogger(aws.NewDefaultLogger()).
	//WithLogLevel(aws.LogDebug))
}

func (d *driver) accessKey() string {
	return d.config.GetString("efs.accessKey")
}

func (d *driver) secretKey() string {
	return d.config.GetString("efs.secretKey")
}

func (d *driver) securityGroups() []string {
	return strings.Split(d.config.GetString("efs.securityGroups"), ",")
}

func (d *driver) region() string {
	return d.config.GetString("efs.region")
}

func (d *driver) tag() string {
	return d.config.GetString("efs.tag")
}
