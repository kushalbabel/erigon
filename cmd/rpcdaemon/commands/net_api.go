package commands

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/common/hexutil"
)

// NetAPI the interface for the net_ RPC commands
type NetAPI interface {
	Listening(_ context.Context) (bool, error)
	Version(_ context.Context) (string, error)
	PeerCount(_ context.Context) (hexutil.Uint, error)
}

// NetAPIImpl data structure to store things needed for net_ commands
type NetAPIImpl struct {
	ethBackend services.ApiBackend
}

// NewNetAPIImpl returns NetAPIImplImpl instance
func NewNetAPIImpl(eth services.ApiBackend) *NetAPIImpl {
	return &NetAPIImpl{
		ethBackend: eth,
	}
}

// Listening implements net_listening. Returns true if client is actively listening for network connections.
// TODO: Remove hard coded value
func (api *NetAPIImpl) Listening(_ context.Context) (bool, error) {
	return true, nil
}

// Version implements net_version. Returns the current network id.
func (api *NetAPIImpl) Version(ctx context.Context) (string, error) {
	if api.ethBackend == nil {
		// We're running in --datadir mode or otherwise cannot get the backend
		return "", fmt.Errorf(NotAvailableChainData, "net_version")
	}

	res, err := api.ethBackend.NetVersion(ctx)
	if err != nil {
		return "", err
	}

	return strconv.FormatUint(res, 10), nil
}

// PeerCount implements net_peerCount. Returns number of peers currently connected to the client.
// TODO: This routine currently returns a hard coded value of '25'
func (api *NetAPIImpl) PeerCount(_ context.Context) (hexutil.Uint, error) {
	return hexutil.Uint(25), nil
}
