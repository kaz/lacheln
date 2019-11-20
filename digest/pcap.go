package digest

import (
	"fmt"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
)

type (
	PcapQuerySource struct {
		handle *pcap.Handle
	}
)

func NewPcapQuerySource(path string) (QuerySource, error) {
	handle, err := pcap.OpenOffline(path)
	if err != nil {
		return nil, fmt.Errorf("pcap.OpenOffline failed: %w", err)
	}

	return &PcapQuerySource{handle}, nil
}

func (qs *PcapQuerySource) Query() chan string {
	pSrc := gopacket.NewPacketSource(qs.handle, qs.handle.LinkType())

	ch := make(chan string)
	go func() {
		for packet := range pSrc.Packets() {
			layer := packet.ApplicationLayer()
			if layer == nil {
				continue
			}

			content := layer.LayerContents()
			if content[4] != 3 {
				continue
			}

			ch <- string(content[5:])
		}
		close(ch)
	}()

	return ch
}

func (qs *PcapQuerySource) Close() error {
	qs.handle.Close()
	return nil
}
