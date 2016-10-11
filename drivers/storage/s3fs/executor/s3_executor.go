package executor

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/akutz/gofig"

	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/s3fs"
)

// driver is the storage executor for the s3fs storage driver.
type driver struct {
	config gofig.Config
}

const (
	idDelimiter     = "/"
	mountinfoFormat = "%d %d %d:%d %s %s %s %s"
)

func init() {
	registry.RegisterStorageExecutor(s3fs.Name, newDriver)
}

func newDriver() types.StorageExecutor {
	return &driver{}
}

func (d *driver) Init(ctx types.Context, config gofig.Config) error {
	d.config = config
	return nil
}

func (d *driver) Name() string {
	return s3fs.Name
}

// InstanceID returns the local instance ID for the test
func InstanceID() (*types.InstanceID, error) {
	return newDriver().InstanceID(nil, nil)
}

// InstanceID returns the aws instance configuration
func (d *driver) InstanceID(
	ctx types.Context,
	opts types.Store) (*types.InstanceID, error) {

	return &types.InstanceID{
		Driver: d.Name(),
		ID:     "some-instance-id",
	}, nil
}

func (d *driver) NextDevice(
	ctx types.Context,
	opts types.Store) (string, error) {
	return "", types.ErrNotImplemented
}

func (d *driver) LocalDevices(
	ctx types.Context,
	opts *types.LocalDevicesOpts) (*types.LocalDevices, error) {

	//mtt, err := parseMountTable()
	//if err != nil {
	//return nil, err
	//}

	//idmnt := make(map[string]string)
	//for _, mt := range mtt {
	//idmnt[mt.Source] = mt.MountPoint
	//}

	return &types.LocalDevices{
		Driver: s3fs.Name,
		//DeviceMap: idmnt,
		DeviceMap: map[string]string{},
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

var _ = (types.StorageExecutor)(nil)
