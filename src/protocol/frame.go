package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	Magic = 0x52

	FrameHandshake    byte = 0x01
	FrameHandshakeAck byte = 0x02
	FrameData         byte = 0x03
	FrameClose        byte = 0x04

	StatusOK              byte = 0x00
	StatusAuthFailed      byte = 0x01
	StatusTunnelNotFound  byte = 0x02
	StatusProtocolMismatch byte = 0x03
	StatusInternalError   byte = 0x04

	TransportTCP  byte = 0x01
	TransportUDP  byte = 0x02
	TransportAuto byte = 0x03

	InnerProtoTCP byte = 0x01
	InnerProtoUDP byte = 0x02

	CloseNormal           byte = 0x00
	CloseTimeout          byte = 0x01
	CloseTargetUnreachable byte = 0x02
	CloseProtocolError    byte = 0x03
)

const HeaderSize = 4

type Frame struct {
	Type    byte
	Payload []byte
}

type Handshake struct {
	TunnelName     string
	Passwd         string
	TransportProto byte
}

type HandshakeAck struct {
	StatusCode    byte
	AcceptedProto byte
}

type Data struct {
	SessionID    uint32
	InnerProto   byte
	InnerPayload []byte
}

type CloseFrame struct {
	SessionID uint32
	Reason    byte
}

func EncodeFrame(f *Frame) []byte {
	buf := make([]byte, HeaderSize+len(f.Payload))
	buf[0] = Magic
	buf[1] = f.Type
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(f.Payload)))
	copy(buf[HeaderSize:], f.Payload)
	return buf
}

func WriteFrame(w io.Writer, f *Frame) error {
	_, err := w.Write(EncodeFrame(f))
	return err
}

func ReadFrame(r io.Reader) (*Frame, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("failed to read frame header: %w", err)
	}

	if header[0] != Magic {
		return nil, fmt.Errorf("invalid magic byte: 0x%02x, expected 0x%02x", header[0], Magic)
	}

	f := &Frame{
		Type: header[1],
	}
	length := binary.BigEndian.Uint16(header[2:4])

	if length > 0 {
		f.Payload = make([]byte, length)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return nil, fmt.Errorf("failed to read frame payload (len=%d): %w", length, err)
		}
	}

	return f, nil
}

func ParseFrameFromDatagram(data []byte) (*Frame, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("datagram too short: %d bytes, minimum %d", len(data), HeaderSize)
	}

	if data[0] != Magic {
		return nil, fmt.Errorf("invalid magic byte: 0x%02x, expected 0x%02x", data[0], Magic)
	}

	f := &Frame{
		Type: data[1],
	}
	length := binary.BigEndian.Uint16(data[2:4])

	if int(HeaderSize+length) != len(data) {
		return nil, fmt.Errorf("datagram length mismatch: header says %d, actual payload %d", length, len(data)-HeaderSize)
	}

	if length > 0 {
		f.Payload = make([]byte, length)
		copy(f.Payload, data[HeaderSize:])
	}

	return f, nil
}

func EncodeHandshake(h *Handshake) []byte {
	tunnelNameLen := len(h.TunnelName)
	passwdLen := len(h.Passwd)

	if tunnelNameLen > 255 {
		tunnelNameLen = 255
	}
	if passwdLen > 255 {
		passwdLen = 255
	}

	payload := make([]byte, 4+tunnelNameLen+passwdLen)
	payload[0] = byte(tunnelNameLen)
	payload[1] = byte(passwdLen)
	payload[2] = h.TransportProto
	payload[3] = 0x00 // Reserved
	copy(payload[4:4+tunnelNameLen], h.TunnelName[:tunnelNameLen])
	copy(payload[4+tunnelNameLen:], h.Passwd[:passwdLen])

	return payload
}

func DecodeHandshake(payload []byte) (*Handshake, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("handshake payload too short: %d bytes", len(payload))
	}

	tunnelNameLen := int(payload[0])
	passwdLen := int(payload[1])
	transportProto := payload[2]

	expectedLen := 4 + tunnelNameLen + passwdLen
	if len(payload) < expectedLen {
		return nil, fmt.Errorf("handshake payload incomplete: expected %d, got %d", expectedLen, len(payload))
	}

	h := &Handshake{
		TunnelName:     string(payload[4 : 4+tunnelNameLen]),
		Passwd:         string(payload[4+tunnelNameLen : 4+tunnelNameLen+passwdLen]),
		TransportProto: transportProto,
	}

	return h, nil
}

func EncodeHandshakeAck(ack *HandshakeAck) []byte {
	payload := make([]byte, 4)
	payload[0] = ack.StatusCode
	payload[1] = ack.AcceptedProto
	payload[2] = 0x00 // Reserved
	payload[3] = 0x00 // Reserved
	return payload
}

func DecodeHandshakeAck(payload []byte) (*HandshakeAck, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("handshake ack payload too short: %d bytes", len(payload))
	}

	return &HandshakeAck{
		StatusCode:    payload[0],
		AcceptedProto: payload[1],
	}, nil
}

func EncodeData(d *Data) []byte {
	payload := make([]byte, 5+len(d.InnerPayload))
	binary.BigEndian.PutUint32(payload[0:4], d.SessionID)
	payload[4] = d.InnerProto
	copy(payload[5:], d.InnerPayload)
	return payload
}

func DecodeData(payload []byte) (*Data, error) {
	if len(payload) < 5 {
		return nil, fmt.Errorf("data payload too short: %d bytes", len(payload))
	}

	return &Data{
		SessionID:    binary.BigEndian.Uint32(payload[0:4]),
		InnerProto:   payload[4],
		InnerPayload: payload[5:],
	}, nil
}

func EncodeClose(cf *CloseFrame) []byte {
	payload := make([]byte, 5)
	binary.BigEndian.PutUint32(payload[0:4], cf.SessionID)
	payload[4] = cf.Reason
	return payload
}

func DecodeClose(payload []byte) (*CloseFrame, error) {
	if len(payload) < 5 {
		return nil, fmt.Errorf("close payload too short: %d bytes", len(payload))
	}

	return &CloseFrame{
		SessionID: binary.BigEndian.Uint32(payload[0:4]),
		Reason:    payload[4],
	}, nil
}
