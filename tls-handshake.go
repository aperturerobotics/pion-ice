// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build !tinygo

package ice

import (
	"context"
	"crypto/tls"
)

func handshakeTLSConn(ctx context.Context, conn *tls.Conn) error {
	return conn.HandshakeContext(ctx)
}
