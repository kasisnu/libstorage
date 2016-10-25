package glusterfs

import (
	"github.com/akutz/gofig"
)

const (
	// Name is the provider's name.
	Name = "glusterfs"

	// InstanceIDFieldRegion is the key to retrieve the region value from the
	// InstanceID Field map.
	InstanceIDFieldRegion = "region"

	// InstanceIDFieldAvailabilityZone is the key to retrieve the availability
	// zone value from the InstanceID Field map.
	InstanceIDFieldAvailabilityZone = "availabilityZone"
)

func init() {
	registerConfig()
}

func registerConfig() {
	r := gofig.NewRegistration("GLUSTERFS")
	r.Key(gofig.String, "", "%s.gluster", "", "glusterfs.connectionStringFormatter")
	gofig.Register(r)
}
