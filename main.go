package main

import (
  "encoding/csv"
  "github.com/Vrex123/go_scraper/scraper"
  "github.com/ilyakaznacheev/cleanenv"
  "log/slog"
  "os"
)

type Config struct {
  RetryCount       int    `env:"RETRY_COUNT" env-default:"3"`
  Timeout          string `env:"TIMEOUT" env-default:"10s"`
  ParallelReqCount int    `env:"PARALLEL_REQ_COUNT" env-default:"10"`
  CsvFilename      string `env:"CSV_FILENAME" env-default:"result.csv"`
}

func main() {
  var cfg Config

  err := cleanenv.ReadEnv(&cfg)

  if err != nil {
    slog.Error("Failed to read config", "err", err)
  }

  slog.Info("Starting app")
  in := readUrls()

  s, err := scraper.NewScraper(cfg.Timeout, cfg.RetryCount, cfg.ParallelReqCount)
  if err != nil {
    slog.Error("Failed to create scraper", "err", err)
    panic(err)
  }

  out := s.Scrape(in)

  writeCsv(&cfg, out)

  slog.Info("App finished")
}

func readUrls() <-chan string {
  c := make(chan string)

  go func() {
    for _, url := range os.Args[1:] {
      c <- url
    }

    close(c)
  }()

  return c
}

func writeCsv(cfg *Config, out <-chan []string) {
  file, err := os.Create(cfg.CsvFilename)
  if err != nil {
    slog.Error("Failed to create file", "err", err)
    panic(err)
  }

  w := csv.NewWriter(file)
  defer w.Flush()

  for r := range out {
    err := w.Write(r)
    if err != nil {
      slog.Error("Failed to write to file", "err", err, "row", r)
      panic(err)
    }
  }
}
