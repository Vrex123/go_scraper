package scraper

import (
  "fmt"
  "golang.org/x/net/html"
  "golang.org/x/net/html/atom"
  "io"
  "log/slog"
  "net/http"
  "time"
)

type Scraper struct {
  Timeout          time.Duration
  Retry            int
  ParallelReqCount int
}

var FailedToFetchUrl = fmt.Errorf("failed to fetch url")

func NewScraper(timeout string, retryCount int, parallelReqCount int) (*Scraper, error) {
  t, err := time.ParseDuration(timeout)
  if err != nil {
    return nil, err
  }

  return &Scraper{
    Timeout:          t,
    Retry:            retryCount,
    ParallelReqCount: parallelReqCount,
  }, nil
}

func (s *Scraper) Scrape(in <-chan string) <-chan []string {
  out := make(chan []string)

  go func() {
    sig := make(chan struct{})

    requestCounter := 0

    for {
      if requestCounter >= s.ParallelReqCount {
        slog.Info("Waiting for request to finish")
        <-sig
        requestCounter--
        continue
      }

      url, ok := <-in
      if !ok {
        break
      }

      slog.Info("Scraping url", "url", url)
      requestCounter++
      go s.scrapeUrl(url, out, sig)
    }

    if requestCounter > 0 {
      for range sig {
        requestCounter--

        if requestCounter == 0 {
          break
        }
      }
    }

    close(sig)
    close(out)
  }()

  return out
}

func (s *Scraper) scrapeUrl(url string, c chan<- []string, sig chan<- struct{}) {
  buf, statusCode, err := s.fetchURL(url)
  if err != nil {
    slog.Error("Failed to fetch url", "url", url, "err", err)
    sig <- struct{}{}
    return
  }

  defer buf.Close()

  title, description, err := s.parseHtml(buf)

  c <- []string{time.Now().Local().Format(time.DateOnly), url, fmt.Sprintf("%d", statusCode), title, description}
  sig <- struct{}{}
}

func (s *Scraper) fetchURL(url string) (io.ReadCloser, int, error) {
  httpClient := &http.Client{
    Timeout: s.Timeout,
  }

  for i := 0; i < s.Retry; i++ {
    resp, err := httpClient.Get(url)
    if err != nil || resp.StatusCode > 299 {
      if resp != nil {
        closeError := resp.Body.Close()
        if closeError != nil {
          return nil, 0, closeError
        }

        slog.Error("Failed to fetch url", "url", url, "status_code", resp.StatusCode)
      } else {
        slog.Error("Failed to fetch url", "url", url, "err", err)
      }

      time.Sleep(time.Duration(i+1) * time.Second)
      continue
    }

    return resp.Body, resp.StatusCode, nil
  }

  return nil, 0, FailedToFetchUrl
}

func (s *Scraper) parseHtml(r io.ReadCloser) (string, string, error) {
  node, err := html.Parse(r)
  if err != nil {
    return "", "", err
  }

  var title string
  var description string

  for n := range node.Descendants() {
    if n.Type == html.ElementNode && n.Data == "head" {
      for headNode := range n.Descendants() {
        if headNode.Type == html.ElementNode && headNode.DataAtom == atom.Title {
          title = headNode.FirstChild.Data
        } else if headNode.Type == html.ElementNode && headNode.DataAtom == atom.Meta {
          for _, attr := range headNode.Attr {
            if attr.Key == "name" && attr.Val == "description" {
              for _, a := range headNode.Attr {
                if a.Key == "content" {
                  description = a.Val
                }
              }
            }
          }
        }
      }
    }
  }

  return title, description, nil
}
