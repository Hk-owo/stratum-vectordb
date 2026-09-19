// Command stratum-client is a runnable example of the caller's side of
// docs/client-integration-guide.md, built on the client/ package.
//
// It is the whole loop a caller needs: submit a batch, keep its changes locally,
// wait for it to land, and — when it never will — either re-send under the same
// key or discard the version.
//
// What it does NOT do is decide for you. `await` prints the decision it reached
// and stops; only `-follow` acts on it. A tool that silently re-sent or discarded
// would be making the caller's decision for it, which is the one thing the whole
// design keeps on the caller's side (docs/await-version-plan.md §2.1).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"stratum/client"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7009", "服务站的 gRPC 地址（没有服务站时用任一控制节点）")
	state := flag.String("state", ".stratum-client", "本地状态目录（客户端自己那份 changes 就存在这里）")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	c, err := client.Dial(*addr, *state)
	if err != nil {
		fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	switch args[0] {
	case "kb-create":
		cmdKBCreate(ctx, c, args[1:])
	case "submit":
		cmdSubmit(ctx, c, args[1:])
	case "list":
		cmdList(c)
	case "await":
		cmdAwait(ctx, c, args[1:])
	case "resend":
		cmdResend(ctx, c, args[1:])
	case "discard":
		cmdDiscard(ctx, c, args[1:])
	case "forget":
		cmdForget(c, args[1:])
	case "query":
		cmdQuery(ctx, c, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "未知命令 %q\n", args[0])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `用法: stratum-client [全局选项] <命令> [命令选项]

全局选项:
  -addr   服务站 gRPC 地址            (默认 127.0.0.1:7009)
  -state  本地状态目录                 (默认 .stratum-client)

命令:
  kb-create -name 名称 [-embed URL] [-model ID]
  submit    -kb <知识库ID> -changes <changes.json>
  list
  await     -id <本地记录ID> [-follow] [-timeout 5m]
  resend    -id <本地记录ID>
  discard   -id <本地记录ID>
  forget    -id <本地记录ID>          # 丢掉本地那份 changes（模拟"客户端把数据弄丢了"）
  query     -kb <知识库ID> -vector 0.1,0.2,… [-version N] [-top-k 5]

changes.json 与 scripts/ops 一样：
  [{"op":"ADD","doc_id":"d1","content":"整篇全文"}, {"op":"DELETE","doc_id":"d2"}]
`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "错误:", err)
	os.Exit(1)
}

func cmdKBCreate(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("kb-create", flag.ExitOnError)
	name := fs.String("name", "", "知识库名称（必填）")
	embed := fs.String("embed", "http://localhost:8080", "embed 服务地址")
	model := fs.String("model", "mock-embed-v1", "embed 模型 ID（与查询向量必须一致）")
	_ = fs.Parse(args)
	if *name == "" {
		fatal(fmt.Errorf("kb-create 需要 -name"))
	}

	kbID, err := c.CreateKnowledgeBase(ctx, *name, *embed, *model)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("知识库已创建：%s（首个版本会在你第一次 submit 时出现）\n", kbID)
}

// changesFile reads the same shape scripts/ops/kb-version-create.sh accepts: a
// bare array, or an object with a "changes" field.
func changesFile(path string) ([]client.Change, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var bare []client.Change
	if err := json.Unmarshal(data, &bare); err == nil {
		return bare, nil
	}
	var wrapped struct {
		Changes []client.Change `json:"changes"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return nil, fmt.Errorf("%s 既不是 changes 数组，也不是 {\"changes\": [...]}: %w", path, err)
	}
	return wrapped.Changes, nil
}

func cmdSubmit(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	kb := fs.String("kb", "", "知识库 ID（必填）")
	file := fs.String("changes", "", "changes JSON 文件（必填）")
	_ = fs.Parse(args)
	if *kb == "" || *file == "" {
		fatal(fmt.Errorf("submit 需要 -kb 与 -changes"))
	}

	changes, err := changesFile(*file)
	if err != nil {
		fatal(err)
	}
	w, err := c.Submit(ctx, *kb, changes)
	printRecord(w)
	if err != nil {
		// The record is on disk even though the call failed: that is the point.
		fmt.Fprintln(os.Stderr, "错误:", err)
		fmt.Fprintf(os.Stderr, "提示：记录 %s 已落盘（含 changes 与幂等键），可以直接 resend -id %s\n", w.ID, w.ID)
		os.Exit(1)
	}
	fmt.Printf("已提交，版本 %d。用 await -id %s 等它就绪。\n", w.VersionID, w.ID)
}

func cmdList(c *client.Client) {
	records, err := c.Store().Load()
	if err != nil {
		fatal(err)
	}
	if len(records) == 0 {
		fmt.Println("本地没有在途记录")
		return
	}
	for _, w := range records {
		state := "在途"
		if w.Settled {
			state = "已结束"
		}
		changes := fmt.Sprintf("%d 条 changes", len(w.Changes))
		if len(w.Changes) == 0 {
			changes = "changes 已不在本地"
		}
		fmt.Printf("%s  kb=%s  version=%d  %s  %s  key=%s\n",
			w.ID, w.KnowledgeBaseID, w.VersionID, changes, state, w.ClientRequestID)
	}
}

func cmdAwait(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("await", flag.ExitOnError)
	id := fs.String("id", "", "本地记录 ID（必填）")
	follow := fs.Bool("follow", false, "按决策行事（重发 / 放弃）；默认只报告")
	timeout := fs.Duration("timeout", 5*time.Minute, "最多等多久")
	_ = fs.Parse(args)
	if *id == "" {
		fatal(fmt.Errorf("await 需要 -id"))
	}

	decision, resp, err := c.WaitUntilSettled(ctx, *id, *timeout)
	if resp != nil {
		fmt.Printf("stage=%s retry_after_ms=%d data_missing=%v → 建议动作 %s\n",
			resp.GetStage(), resp.GetRetryAfterMs(), resp.GetDataMissing(), decision)
	}
	if err != nil {
		// Unfinished is not a failure of the wait: say what we know and exit.
		fmt.Fprintln(os.Stderr, "注意:", err)
		os.Exit(1)
	}

	switch decision {
	case client.DecisionDone:
		fmt.Println("写入已就绪，可以查询/激活了。")
		_ = c.MarkSettled(*id)
	case client.DecisionWait:
		fmt.Println("仍在进行中（服务端没有说它不会完成）。")
	case client.DecisionResend:
		fmt.Println("数据没有任何可达副本持有，而本地还有 changes —— 应当**用同一 key 重发**。")
		if *follow {
			w, err := c.Resend(ctx, *id)
			if err != nil {
				fatal(err)
			}
			fmt.Printf("已重发，版本 %d。\n", w.VersionID)
		} else {
			fmt.Printf("（加 -follow 会自动执行：resend -id %s）\n", *id)
		}
	case client.DecisionDiscard:
		fmt.Println("这个版本不会再落地，且本地已经没有 changes —— 只剩**放弃**。")
		if *follow {
			if err := c.Discard(ctx, *id); err != nil {
				fatal(err)
			}
			fmt.Println("已放弃该版本。")
		} else {
			fmt.Printf("（加 -follow 会自动执行：discard -id %s）\n", *id)
		}
	}
}

func cmdResend(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("resend", flag.ExitOnError)
	id := fs.String("id", "", "本地记录 ID（必填）")
	_ = fs.Parse(args)
	if *id == "" {
		fatal(fmt.Errorf("resend 需要 -id"))
	}
	w, err := c.Resend(ctx, *id)
	printRecord(w)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("已按同一幂等键重发，拿回版本 %d（同一 key 必须拿回同一个版本）。\n", w.VersionID)
}

func cmdDiscard(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("discard", flag.ExitOnError)
	id := fs.String("id", "", "本地记录 ID（必填）")
	_ = fs.Parse(args)
	if *id == "" {
		fatal(fmt.Errorf("discard 需要 -id"))
	}
	if err := c.Discard(ctx, *id); err != nil {
		fatal(err)
	}
	fmt.Println("已放弃该版本（它从未落地，服务端不会为此回收任何数据）。")
}

func cmdForget(c *client.Client, args []string) {
	fs := flag.NewFlagSet("forget", flag.ExitOnError)
	id := fs.String("id", "", "本地记录 ID（必填）")
	_ = fs.Parse(args)
	if *id == "" {
		fatal(fmt.Errorf("forget 需要 -id"))
	}
	if err := c.ForgetChanges(*id); err != nil {
		fatal(err)
	}
	fmt.Println("本地 changes 已丢弃；这条记录之后只能放弃，不能重发。")
}

func cmdQuery(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	kb := fs.String("kb", "", "知识库 ID（必填）")
	vec := fs.String("vector", "", "查询向量（逗号分隔；由你自己 embed）")
	version := fs.Int64("version", 0, "版本（0 = 激活版本）")
	topK := fs.Int("top-k", 5, "返回条数")
	_ = fs.Parse(args)
	if *kb == "" || *vec == "" {
		fatal(fmt.Errorf("query 需要 -kb 与 -vector"))
	}

	parts := strings.Split(*vec, ",")
	vector := make([]float32, 0, len(parts))
	for _, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			fatal(fmt.Errorf("向量分量 %q 不是数字: %w", p, err))
		}
		vector = append(vector, float32(f))
	}

	resp, err := c.Query(ctx, *kb, *version, vector, *topK)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("命中的版本 %d，%d 条结果\n", resp.GetVersionId(), len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		fmt.Printf("  %.4f  %s  %s\n", r.GetScore(), r.GetDocId(), truncate(r.GetContent(), 60))
	}
}

func printRecord(w client.PendingWrite) {
	fmt.Printf("记录 %s：kb=%s version=%d key=%s\n", w.ID, w.KnowledgeBaseID, w.VersionID, w.ClientRequestID)
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
