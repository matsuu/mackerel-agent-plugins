package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollectWho(t *testing.T) {
	_, err := os.Stat("/usr/bin/who")
	if err == nil {
		return
	}
	p := make(map[string]float64)

	assert.Nil(t, collectWho(&p))
}

func TestParseWho(t *testing.T) {
	stub := `test0  pts/48       2014-09-30 08:00 (192.168.24.123)
test1  pts/48       2014-09-30 08:59 (192.168.24.123)
test2  pts/48       2014-09-30 09:00 (192.168.24.123)`
	stat := make(map[string]float64)

	err := parseWho(stub, &stat)
	assert.Nil(t, err)
	assert.Equal(t, stat["users"], 3)
}

func TestParseWho2(t *testing.T) {
	stub := ""
	stat := make(map[string]float64)

	err := parseWho(stub, &stat)
	assert.Nil(t, err)
	assert.Equal(t, stat["users"], 0)
}

func TestGetWho(t *testing.T) {
	_, err := os.Stat("/usr/sbin/who")
	if err == nil {
		return
	}

	ret, err := getWho()
	assert.Nil(t, err)
	assert.NotNil(t, ret)
}

func TestCollectStat(t *testing.T) {
	path := "/proc/stat"
	_, err := os.Stat(path)
	if err == nil {
		return
	}
	p := make(map[string]float64)

	assert.Nil(t, collectProcStat(path, &p))
}

func TestParseProcStat(t *testing.T) {
	stub := `intr 614818624 122 8 0 0 1 0 0 0 1 0 0 0 123 0 0 0 0 0 0 0 0 0 0 0 4846888 0 44650320 253 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
 ctxt 879305394
 btime 1409212617
 processes 1959410`
	stat := make(map[string]float64)

	err := parseProcStat(stub, &stat)
	assert.Nil(t, err)
	assert.Equal(t, stat["interrupts"], 614818624)
	assert.Equal(t, stat["context_switches"], 879305394)
	assert.Equal(t, stat["forks"], 1959410)
}

func TestCollectProcDiskstats(t *testing.T) {
	path := "/proc/diskstats"
	_, err := os.Stat(path)
	if err == nil {
		return
	}
	p := make(map[string]float64)

	assert.Nil(t, collectProcDiskstats(path, &p))
}

func TestParseProcDiskstats(t *testing.T) {
	stub := `   1       0 ram0 0 0 0 0 0 0 0 0 0 0 0
   8       0 sda 324351 303093 35032074 12441261 4456146 5387174 68639686 423711425 0 23865772 436201338
   8       1 sda1 678 405 10970 4696 276 22946 46462 1217036 0 53528 1221732
 253       2 dm-2 83 0 664 94 0 0 0 0 0 94 94`
	stat := make(map[string]float64)

	err := parseProcDiskstats(stub, &stat)
	assert.Nil(t, err)
	assert.Equal(t, stat["iotime_sda"], 23865772)
	assert.Equal(t, stat["iotime_weighted_sda"], 436201338)
	assert.Equal(t, stat["tsreading_sda"], 12441261)
	assert.Equal(t, stat["tswriting_sda"], 423711425)
}

func TestCollectSs(t *testing.T) {
	_, err := os.Stat("/usr/sbin/ss")
	if err == nil {
		return
	}
	p := make(map[string]float64)

	assert.Nil(t, collectSs(&p))
}

func TestParseSs(t *testing.T) {
	stub := `State      Recv-Q Send-Q                       Local Address:Port                         Peer Address:Port 
LISTEN     0      128                                     :::45103                                  :::*     
LISTEN     0      128                                     :::111                                    :::* 
TIME-WAIT  0      0                         ::ffff:127.0.0.1:80                       ::ffff:127.0.0.1:50082 
ESTAB      0      0                              10.0.25.101:60826                         10.0.25.104:5672  `
	stat := make(map[string]float64)

	err := parseSs(stub, &stat)
	assert.Nil(t, err)
	assert.Equal(t, stat["LISTEN"], 2)
	assert.Equal(t, stat["TIME-WAIT"], 1)
	assert.Equal(t, stat["ESTAB"], 1)
}

func TestGetSs(t *testing.T) {
	_, err := os.Stat("/usr/sbin/ss")
	if err == nil {
		return
	}

	ret, err := getSs()
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Contains(t, ret, "Stats")
}

func TestCollectProcVmstat(t *testing.T) {
	path := "/proc/vmstat"
	_, err := os.Stat(path)
	if err == nil {
		return
	}
	p := make(map[string]float64)

	assert.Nil(t, collectProcVmstat(path, &p))
}

func TestParseProcVmstat(t *testing.T) {
	stub := `pgpgin 770294
pgpgout 31351354
pswpin 0
pswpout 113`
	stat := make(map[string]float64)

	err := parseProcVmstat(stub, &stat)
	assert.Nil(t, err)
	assert.Equal(t, stat["pgpgin"], 770294)
	assert.Equal(t, stat["pgpgout"], 31351354)
	assert.Equal(t, stat["pswpin"], 0)
	assert.Equal(t, stat["pswpout"], 113)
}

func TestGetProc(t *testing.T) {
	stub := "/proc/diskstats"
	_, err := os.Stat(stub)
	if err == nil {
		return
	}

	ret, err := getProc(stub)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Contains(t, ret, "ram0")
}
