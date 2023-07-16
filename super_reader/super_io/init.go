package super_io

import (
	"os"
	//"time"

	"github.com/rs/zerolog"
)

var (
    mLog = zerolog.New(os.Stdout).With().Str("module", "super_io").Timestamp().Logger()
)

func init() {
}
