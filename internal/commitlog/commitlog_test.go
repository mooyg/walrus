package commitlog

import (
	"os"
	"testing"

	logger "github.com/mooyg/walrus/internal/log"
)

func TestMain(m *testing.M) {
	logger.Init("error")
	os.Exit(m.Run())
}

func TestOpen(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "creates directory and file",
			path:    "testdata/test.log",
			wantErr: false,
		},
		{
			name:    "opens existing file",
			path:    "testdata/existing.log",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer os.RemoveAll("testdata")

			fl, err := Open(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				defer fl.Close()
				if fl.offset != 0 {
					t.Errorf("Open() offset = %d, want 0", fl.offset)
				}
			}
		})
	}
}

func TestAppend(t *testing.T) {
	defer os.RemoveAll("testdata")

	fl, err := Open("testdata/append.log")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer fl.Close()

	tests := []struct {
		name   string
		data   []byte
		wantOk bool
	}{
		{
			name:   "append single message",
			data:   []byte("hello"),
			wantOk: true,
		},
		{
			name:   "append another message",
			data:   []byte("world"),
			wantOk: true,
		},
		{
			name:   "append empty data",
			data:   []byte{},
			wantOk: true,
		},
	}

	offset := int64(0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fl.Append(tt.data)
			if err != nil {
				t.Errorf("Append() error = %v", err)
				return
			}
			if got != offset {
				t.Errorf("Append() offset = %d, want %d", got, offset)
			}
			offset++
		})
	}
}

func TestReopenPreservesOffset(t *testing.T) {
	defer os.RemoveAll("testdata")

	path := "testdata/reopen.log"

	fl1, err := Open(path)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}

	fl1.Append([]byte("msg1"))
	fl1.Append([]byte("msg2"))
	fl1.Close()

	fl2, err := Open(path)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer fl2.Close()

	if fl2.offset != 2 {
		t.Errorf("Open() offset = %d, want 2", fl2.offset)
	}
}

func TestAppendAndReopen(t *testing.T) {
	defer os.RemoveAll("testdata")

	path := "testdata/persist.log"

	fl1, err := Open(path)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}

	offset1, err := fl1.Append([]byte("first"))
	if err != nil {
		t.Fatalf("Append() failed: %v", err)
	}
	if offset1 != 0 {
		t.Errorf("first Append() offset = %d, want 0", offset1)
	}

	fl1.Close()

	fl2, err := Open(path)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer fl2.Close()

	offset2, err := fl2.Append([]byte("second"))
	if err != nil {
		t.Fatalf("Append() failed: %v", err)
	}
	if offset2 != 1 {
		t.Errorf("second Append() offset = %d, want 1", offset2)
	}

	if fl2.offset != 2 {
		t.Errorf("final offset = %d, want 2", fl2.offset)
	}
}

func TestClose(t *testing.T) {
	defer os.RemoveAll("testdata")

	fl, err := Open("testdata/close.log")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}

	fl.Append([]byte("test"))

	err = fl.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	_, err = fl.file.Stat()
	if err == nil {
		t.Errorf("Close() file should be closed")
	}
}
