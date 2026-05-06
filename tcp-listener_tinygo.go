// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build tinygo

package ice

import "net"

func listenTCPAddrOnInterface(address string) (*net.TCPAddr, error) {
	return net.ResolveTCPAddr("tcp", address)
}
