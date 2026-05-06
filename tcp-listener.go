// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build !tinygo

package ice

import "net"

func listenTCPAddrOnInterface(address string) (*net.TCPAddr, error) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = l.Close()
	}()

	tcpAddr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return nil, errInvalidAddress
	}

	return tcpAddr, nil
}
