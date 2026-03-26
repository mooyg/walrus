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

func TestReadFrom(t *testing.T) {
	defer os.RemoveAll("testdata")

	fl, err := Open("testdata/readfrom.log")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer fl.Close()

	fl.Append([]byte("hello"))
	fl.Append([]byte("world"))
	fl.Append([]byte("foo"))

	t.Run("read all from start", func(t *testing.T) {
		msgs, err := fl.ReadFrom(0, 10)
		if err != nil {
			t.Fatalf("ReadFrom() error: %v", err)
		}
		if len(msgs) != 3 {
			t.Fatalf("got %d messages, want 3", len(msgs))
		}
		if string(msgs[0].Data) != "hello" || string(msgs[1].Data) != "world" || string(msgs[2].Data) != "foo" {
			t.Errorf("unexpected data: %v", msgs)
		}
	})

	t.Run("read from middle", func(t *testing.T) {
		msgs, err := fl.ReadFrom(1, 10)
		if err != nil {
			t.Fatalf("ReadFrom() error: %v", err)
		}
		if len(msgs) != 2 {
			t.Fatalf("got %d messages, want 2", len(msgs))
		}
		if string(msgs[0].Data) != "world" || string(msgs[1].Data) != "foo" {
			t.Errorf("unexpected data: %v", msgs)
		}
		if msgs[0].Offset != 1 {
			t.Errorf("first message offset = %d, want 1", msgs[0].Offset)
		}
	})

	t.Run("max limits results", func(t *testing.T) {
		msgs, err := fl.ReadFrom(0, 2)
		if err != nil {
			t.Fatalf("ReadFrom() error: %v", err)
		}
		if len(msgs) != 2 {
			t.Fatalf("got %d messages, want 2", len(msgs))
		}
		if string(msgs[1].Data) != "world" {
			t.Errorf("second message = %q, want \"world\"", msgs[1].Data)
		}
	})

	t.Run("offset out of bounds", func(t *testing.T) {
		msgs, err := fl.ReadFrom(99, 10)
		if err != nil {
			t.Fatalf("ReadFrom() error: %v", err)
		}
		if len(msgs) != 0 {
			t.Errorf("got %d messages, want 0", len(msgs))
		}
	})

	t.Run("negative offset", func(t *testing.T) {
		msgs, err := fl.ReadFrom(-1, 10)
		if err != nil {
			t.Fatalf("ReadFrom() error: %v", err)
		}
		if len(msgs) != 0 {
			t.Errorf("got %d messages, want 0", len(msgs))
		}
	})

	t.Run("read last entry only", func(t *testing.T) {
		msgs, err := fl.ReadFrom(2, 1)
		if err != nil {
			t.Fatalf("ReadFrom() error: %v", err)
		}
		if len(msgs) != 1 {
			t.Fatalf("got %d messages, want 1", len(msgs))
		}
		if string(msgs[0].Data) != "foo" {
			t.Errorf("got %q, want \"foo\"", msgs[0].Data)
		}
	})
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
