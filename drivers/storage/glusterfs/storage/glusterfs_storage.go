package storage

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/akutz/gofig"
	"github.com/akutz/goof"

	"github.com/emccode/libstorage/api/context"
	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/glusterfs"
)

const (
	volPlaceholder = "%s"
)

var (
	ErrMissingVolID   = goof.New("glusterfs: missing volume ID")
	ErrMissingVolName = goof.New("glusterfs: missing volume name")
)

// Driver represents a GlusterFS driver implementation of StorageDriver
type driver struct {
	config  gofig.Config
	volumes map[string]bool
	mu      sync.RWMutex
}

func init() {
	registry.RegisterStorageDriver(glusterfs.Name, newDriver)
}

func newDriver() types.StorageDriver {
	return &driver{}
}

// Name returns the name of the driver
func (d *driver) Name() string {
	return glusterfs.Name
}

// Init initializes the driver.
func (d *driver) Init(ctx types.Context, config gofig.Config) error {
	d.config = config
	d.volumes = map[string]bool{}
	if count := strings.Count(d.formatter(), volPlaceholder); count < 1 {
		return errors.New("formatter must contain at least one %s")
	} else if count > 1 {
		return errors.New("formatter must only one %s")
	}

	fields := log.Fields{
		"formatter": d.formatter(),
	}
	ctx.WithFields(fields).Info("storage driver initialized")
	return nil
}

// InstanceInspect returns an instance.
func (d *driver) InstanceInspect(
	ctx types.Context,
	opts types.Store) (*types.Instance, error) {

	instanceID := &types.InstanceID{ID: d.mockID(), Driver: d.Name()}

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

	return nil, types.ErrNotImplemented

	var volumesSD []*types.Volume

	for volume, _ := range d.volumesRO() {
		vol := &types.Volume{
			Name:        volume,
			ID:          volume,
			Attachments: nil,
		}

		var atts []*types.VolumeAttachment
		var err error
		if opts.Attachments {
			atts, err = d.getVolumeAttachments(ctx, volume)
			if err != nil {
				return nil, err
			}
		}
		if len(atts) > 0 {
			vol.Attachments = atts
		}
		volumesSD = append(volumesSD, vol)
	}

	return volumesSD, nil
}

// VolumeInspect inspects a single volume.
func (d *driver) VolumeInspect(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeInspectOpts) (*types.Volume, error) {

	volume := &types.Volume{
		Name:        volumeID,
		ID:          volumeID,
		Attachments: nil,
	}
	var atts []*types.VolumeAttachment
	var err error
	if opts.Attachments {
		atts, err = d.getVolumeAttachments(ctx, volumeID)
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

	return nil, types.ErrNotImplemented

	if name == "" {
		return nil, ErrMissingVolName
	}

	err := d.volumeAdd(name)
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

	return types.ErrNotImplemented

	if volumeID == "" {
		return ErrMissingVolID
	}

	return d.volumeRemove(volumeID)
}

// VolumeAttach attaches a volume and provides a token clients can use
// to validate that device has appeared locally.
func (d *driver) VolumeAttach(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeAttachOpts) (*types.Volume, string, error) {

	vol, err := d.VolumeInspect(ctx, volumeID,
		&types.VolumeInspectOpts{Attachments: false})

	return vol, "", err
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

func (d *driver) formatter() string {
	return d.config.GetString("glusterfs.connectionStringFormatter")
}

func (d *driver) getVolumeAttachments(ctx types.Context, volumeID string) (
	[]*types.VolumeAttachment, error) {

	if volumeID == "" {
		return nil, ErrMissingVolID
	}

	var dev, status string
	dev = fmt.Sprintf(d.formatter(), volumeID)

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
			InstanceID: &types.InstanceID{ID: d.mockID(), Driver: d.Name()},
			DeviceName: dev,
			Status:     status,
		},
	}

	return attachments, nil
}

func (d *driver) volumesRO() map[string]bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var m = map[string]bool{}
	for k, v := range d.volumes {
		m[k] = v
	}
	return m
}

func (d *driver) volumeAdd(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.volumes[name] = true
	return nil
}

func (d *driver) volumeRemove(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.volumes, name)
	return nil
}

func (d *driver) mockID() string {
	return "an-unused-id"
}
