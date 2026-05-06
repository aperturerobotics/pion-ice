// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build tinygo

package ice

import (
	"context"
	"net"
)

func handshakeTLSConn(_ context.Context, conn *net.TLSConn) error {
	return conn.Handshake()
}
