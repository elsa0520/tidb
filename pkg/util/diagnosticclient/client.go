// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0
package diagnosticclient

import (
	"context"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	"github.com/tikv/pd/client/opt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

func PDClientOption() opt.ClientOption {
	return opt.WithGRPCDialOptions(grpc.WithChainUnaryInterceptor(pdUnary), grpc.WithChainStreamInterceptor(pdStream))
}
func PDClientOptions(opts []opt.ClientOption) []opt.ClientOption {
	if !diagnosticmode.Enabled() {
		return opts
	}
	return append(opts, PDClientOption())
}
func pdUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	switch method {
	case "/pdpb.PD/GetMembers", "/pdpb.PD/GetStore", "/pdpb.PD/GetRegion":
		return inv(ctx, method, req, reply, cc, opts...)
	}
	return status.Errorf(codes.PermissionDenied, "diagnostic mode: blocked PD RPC %s", method)
}
func pdStream(ctx context.Context, d *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if method == "/pdpb.PD/Tso" || method == "/tsopb.TSO/Tso" {
		return streamer(ctx, d, cc, method, opts...)
	}
	return nil, status.Errorf(codes.PermissionDenied, "diagnostic mode: blocked PD stream %s", method)
}

type KVClient struct{ tikv.Client }

func WrapKV(c tikv.Client) tikv.Client {
	if !diagnosticmode.Enabled() {
		return c
	}
	return &KVClient{Client: c}
}
func (c *KVClient) allowed(r *tikvrpc.Request) bool {
	return r != nil && (r.Type == tikvrpc.CmdGet || r.Type == tikvrpc.CmdBatchGet || r.Type == tikvrpc.CmdScan)
}
func (c *KVClient) SendRequest(ctx context.Context, addr string, r *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if !c.allowed(r) {
		return nil, status.Error(codes.PermissionDenied, "diagnostic mode: blocked KV RPC")
	}
	return c.Client.SendRequest(ctx, addr, r, timeout)
}
func (c *KVClient) SendRequestAsync(ctx context.Context, addr string, r *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	if !c.allowed(r) {
		cb.Invoke(nil, status.Error(codes.PermissionDenied, "diagnostic mode: blocked KV RPC"))
		return
	}
	c.Client.SendRequestAsync(ctx, addr, r, cb)
}
