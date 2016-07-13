package executor

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/akutz/gofig"
	"github.com/akutz/goof"

	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/efs"
)

// driver is the storage executor for the efs storage driver.
type driver struct {
	config gofig.Config
}

const (
	idDelimiter     = "/"
	mountinfoFormat = "%d %d %d:%d %s %s %s %s"
)

func init() {
	registry.RegisterStorageExecutor(efs.Name, newDriver)
}

func newDriver() types.StorageExecutor {
	return &driver{}
}

func (d *driver) Init(ctx types.Context, config gofig.Config) error {
	d.config = config
	return nil
}

func (d *driver) Name() string {
	return efs.Name
}

// InstanceID returns the aws instance configuration
func (d *driver) InstanceID(
	ctx types.Context,
	opts types.Store) (*types.InstanceID, error) {

	svc := ec2metadata.New(session.New())
	if !svc.Available() {
		return nil, goof.New("EC2Metadata service not available")
	}

	mac, err := svc.GetMetadata("mac")
	if err != nil {
		return nil, goof.WithError("no ec2metadata mac address", err)
	}

	subnetID, err := svc.GetMetadata(fmt.Sprintf("network/interfaces/macs/%s/subnet-id", mac))
	if err != nil {
		return nil, goof.WithError("no ec2metadata subnet id", err)
	}

	iid := &types.InstanceID{Driver: efs.Name}
	if err := iid.MarshalMetadata(subnetID); err != nil {
		return nil, err
	}

	return iid, nil
}

func (d *driver) NextDevice(
	ctx types.Context,
	opts types.Store) (string, error) {
	return "", types.ErrNotImplemented
}

func (d *driver) LocalDevices(
	ctx types.Context,
	opts *types.LocalDevicesOpts) (*types.LocalDevices, error) {

	mtt, err := parseMountTable()
	if err != nil {
		return nil, err
	}

	// TODO(mhrabovcin): Filter out only AWS NFS mounts?
	idmnt := make(map[string]string)
	for _, mt := range mtt {
		idmnt[mt.Source] = mt.MountPoint
	}

	return &types.LocalDevices{
		Driver:    efs.Name,
		DeviceMap: idmnt,
	}, nil
}

func parseMountTable() ([]*types.MountInfo, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return parseInfoFile(f)
}

func parseInfoFile(r io.Reader) ([]*types.MountInfo, error) {
	var (
		s   = bufio.NewScanner(r)
		out = []*types.MountInfo{}
	)

	for s.Scan() {
		if err := s.Err(); err != nil {
			return nil, err
		}

		var (
			p              = &types.MountInfo{}
			text           = s.Text()
			optionalFields string
		)

		if _, err := fmt.Sscanf(text, mountinfoFormat,
			&p.ID, &p.Parent, &p.Major, &p.Minor,
			&p.Root, &p.MountPoint, &p.Opts, &optionalFields); err != nil {
			return nil, fmt.Errorf("Scanning '%s' failed: %s", text, err)
		}
		// Safe as mountinfo encodes mountpoints with spaces as \040.
		index := strings.Index(text, " - ")
		postSeparatorFields := strings.Fields(text[index+3:])
		if len(postSeparatorFields) < 3 {
			return nil, fmt.Errorf("Error found less than 3 fields post '-' in %q", text)
		}

		if optionalFields != "-" {
			p.Optional = optionalFields
		}

		p.FSType = postSeparatorFields[0]
		p.Source = postSeparatorFields[1]
		p.VFSOpts = strings.Join(postSeparatorFields[2:], " ")
		out = append(out, p)
	}
	return out, nil
}
