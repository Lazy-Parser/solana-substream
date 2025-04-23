package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	pbsub "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"

	mypb "github.com/Lazy-Parser/solana-substream/proto/pb/proto" // protobuf‑тип, который возвращает ваш map‑модуль
)

/* ---------- конфиг ---------- */

var (
	expectedOutputType = string(
		(*mypb.PriceUpdate)(nil).ProtoReflect().Descriptor().FullName(),
	)

	zlog, tracer = logging.RootLogger("price-sink", "github.com/your-org/price-sink")
)

/* ---------- main ---------- */

func main() {
	logging.InstantiateLoggers()

	Run(
		"sinker",
		"Streams PriceUpdate events and logs them",

		Command(runSink,
			"sink <endpoint> <spkg> <module>",
			"Start the sinker",
			RangeArgs(3, 3),
			Flags(func(flags *pflag.FlagSet) { sink.AddFlagsToSet(flags) }),
		),
		OnCommandErrorLogAndExit(zlog),
	)
}

/* ---------- исполнение ---------- */

func runSink(cmd *cobra.Command, args []string) error {
	endpoint, spkgPath, moduleName := args[0], args[1], args[2]

	fmt.Println(expectedOutputType)

	sinker, err := sink.NewFromViper(
		cmd,
		expectedOutputType,
		endpoint,
		spkgPath,
		moduleName,
		"335420000:", // от start_block и «вперёд навсегда»
		zlog,
		tracer,
	)
	cli.NoError(err, "create sinker")

	// graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cursor := sink.NewBlankCursor() // TODO: подставьте восстановление курсора

	sinker.OnTerminating(func(err error) {
		cli.NoError(err, "unexpected sinker error")

		zlog.Info("sink is terminating")
	})

	// запускаем и ждём завершения
	sinker.Run(ctx, cursor, sink.NewSinkerHandlers(onData, onUndo))
	return nil
}

/* ---------- обработчики ---------- */

func onData(_ context.Context, data *pbsub.BlockScopedData, _ *bool, _ *sink.Cursor) error {
	out := &mypb.PriceUpdate{}
	if err := data.Output.MapOutput.UnmarshalTo(out); err != nil {
		return err
	}

	zlog.Info("price update",
		zap.Uint64("block", data.Clock.Number),
		zap.String("pair", out.PairAddress),
		zap.Float64("price_usd", out.PriceUsd),
		zap.Float64("volume_usd", out.VolumeUsd),
	)

	return nil
}

func onUndo(_ context.Context, undo *pbsub.BlockUndoSignal, _ *sink.Cursor) error {
	zlog.Warn("fork undo",
		zap.Uint64("to_block", undo.LastValidBlock.Number),
	)
	return nil
}
