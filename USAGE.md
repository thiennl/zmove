# Hướng dẫn sử dụng zmove

Chạy trên **control node** (máy có SSH tới các mailstore). Mọi lệnh đọc/ghi qua
SSH user `zimbra`, không cần agent trên mailstore.

## 1. Cài đặt

```bash
git clone <repo> /opt/zmove
ln -s /opt/zmove/bin/zmove /usr/local/bin/zmove
zmove --help
```

Cách khác: `pip install /opt/zmove` (lệnh `zmove` vào PATH qua
`console_scripts`). Yêu cầu duy nhất: Python ≥ 3.8, stdlib (không pip package nào).

### Rocky 8 (file thực thi đơn, không cần cài)

```bash
sudo dnf install -y python38      # AppStream; python3 gốc của Rocky 8 (3.6) quá cũ
./dist/zmove.pyz --help          # build bằng ./build_pyz.sh, ~100KB, stdlib-only
sudo cp dist/zmove.pyz /usr/local/bin/zmove
```

`zmove.pyz` là zipapp: chạy trực tiếp mọi nơi có Python ≥ 3.8, không cài đặt,
không phụ thuộc hệ điều hành (đã test `--help`/`query`/`gc` offline).
Muốn ELF thật sự (không cần Python trên máy đích): build trên chính máy
Rocky 8 bằng PyInstaller (`pip install pyinstaller && pyinstaller --onefile
-m zmove` — tương đương, vì code stdlib-only nên đóng gói sạch).

Chuẩn bị SSH (3 hướng, user `zimbra`, key không passphrase hoặc ssh-agent):

```bash
ssh zimbra@<src> true && ssh zimbra@<dst> true
ssh zimbra@<src> "ssh -p 22 zimbra@<dst> true"   # rsync chạy trên source
```

File cấu hình (mặc định `~/.config/zmove/zmove.ini`, hoặc `-c` chỉ file khác):

```ini
[zmove]
ssh_user = zimbra
ssh_port = 22
node_ssh_user = zimbra
node_ssh_port = 22
admin_port = 7071
# admin_user/admin_password bỏ trống = đọc localconfig trên mailstore
state_db = ~/.local/share/zmove/state.db
log_dir = ~/.local/share/zmove/logs
max_syncs = 10
sync_finish_threshold_ms = 180000
max_lock_seconds = 900
```

## 2. Quy trình chuẩn (lần đầu làm tay từng bước)

```bash
# 0. Kiểm tra read-only — sửa hết error mới tiếp
zmove preflight user@dom -f mbx01 -t mbx02

# 1. Chạy thật, dừng hỏi trước mỗi bước nguy hiểm
zmove move user@dom -f mbx01 -t mbx02 --confirm-steps

# 2. Verify (mục 5), rồi mới dọn bản cũ (phải gõ lại đúng email khi hỏi)
zmove purge user@dom -s mbx01
```

## 3. Lệnh theo việc

### Chuyển 1 mailbox (khuyên dùng)

```bash
zmove move user@dom -f mbx01 -t mbx02
zmove move user@dom -f mbx01 -t mbx02 --confirm-steps   # hỏi trước mỗi bước
zmove move user@dom -f mbx01 -t mbx02 --dry-run -v      # in lệnh sẽ chạy, không đổi gì
```

Flag hay dùng: `--max-syncs`, `--sync-finish-threshold` (ms),
`--blobs/--secondary-blobs/--search-index include|exclude|config`,
`--skip-lockout` (server legacy), `--skip-memcache-purge` (proxy quản ngoài),
`--overwrite` (xóa mailbox lạ cùng account ở dest — kiểm tra chủ sở hữu trước
khi xóa), `--exclude-tables event,foo`, `--db-strategy delta|full`,
`--verify full|count|none`, `--cutover-window 02:00-04:00`,
`--max-lock-seconds 900` (watchdog abort+unlock), `--keep-stage` (giữ schema
forensics), `--yes` (bỏ confirm), `--background` (chạy nền).

### Nhờ server NE tự làm (dest phải là NE)

```bash
zmove mboxmove -a user@dom --from mbx01 --to mbx02 [--sync] [--force]
```

Port 1:1 `zmmboxmove`: message, exit code, guard dest y hệt. Không dùng được
khi dest là FOSS (server báo `UNKNOWN_DOCUMENT` + hint).
`--force` = `skipRemoteLockout`, in đúng banner cảnh báo của NE.

### Kiểu NE nguyên bản, không journal (so sánh/học hành, ít dùng prod)

```bash
zmove ne-move user@dom -f mbx01 -t mbx02
```

Full copy DB *while locked*, không resume. Muốn resume/batch → dùng `move`.

### Chạy nhiều

```bash
# plan.csv: account,src,dst (có header, cho phép comment #)
zmove batch plan.csv --parallel 4 --per-server 2 --max-cutovers 2
zmove batch plan.csv --canary 1     # chạy 1 cái trước, fail hoặc decline là dừng
```

### Theo dõi / tra cứu

```bash
zmove query                        # tất cả job
zmove query user@dom --check-peer   # kèm mailHost/status live + peer NE (nếu có)
zmove query --json                 # gồm snapshot điểm xuất phát + data
zmove events <job-id>              # log sự kiện của job
```

### Gỡ kẹt

```bash
zmove resume <job>     # chạy tiếp từ phase đã commit (kể cả resume khi đang LOCKED)
zmove abort <job>      # chỉ trước FLIPPED; xóa bản đích, mở lock source
zmove rollback <job>   # sau FLIPPED, trước PURGED; từ chối nếu dest đã có mail mới
zmove unlock user@dom -s <server>   # account kẹt maintenance (khôi phục status gốc)
zmove gc [--older-than 24] [--prune-events 90]   # dọn job hỏng/aborted (cron weekly có sẵn unit systemd/)
```

## 4. Thoát mã & dữ liệu ở đâu

- `0` xong; `1` fail/abort/decline (kể cả thiếu arg — thống nhất kiểu NE);
  `2` lỗi config/CLI dùng sai chỗ batch.
- Journal SQLite: `state_db` (mặc định `~/.local/share/zmove/state.db`).
- Log: `log_dir` + `zmove events <job>`.
- Metrics textfile Prometheus nếu cấu hình `metrics_textfile`.

## 5. Checklist sau mỗi move (đừng bỏ)

1. `zmove query <acct>` → `DONE`; `query --json` đối chiếu `snapshot`.
2. `zmprov -l ga <acct> zimbraMailHost` == dest.
3. Login webmail vào dest OK; IMAP UID/search/tag/share còn nguyên
   (so `COUNT(*)` + vài UID mẫu trước/sau).
4. Gửi/nhận 1 mail (postqueue đã flush tự động trừ `--no-flush-mta`).
5. Chỉ `purge` khi đã chắc — purge không rollback được, muốn quay lại phải
   move ngược.

## 6. Lỗi hay gặp

| Báo lỗi | Nghĩa / làm gì |
|---|---|
| `zimbraMailHost is X, not SRC` | Sai `-f`; sửa lại đúng home hiện tại |
| `... not a valid mailclient server ...` | Dest sai tên hoặc thiếu service mailbox |
| `admin SOAP auth failed` | Sai user/pass, 7071 chặn, thử curl `https://dst:7071/service/admin/soap/` |
| `stale accountId->mailboxId` | JVM dst còn cache bản cũ → chạy đúng lệnh in trong message (`zmmailboxdctl restart` ở dst) rồi preflight lại |
| `version/db_version/index_version differs` | Hai node khác bản Zimbra — đồng bộ trước, đừng `--force` |
| `ALREADY_BEING_MOVED_OUT` | Đang có move NE khác giữ — chờ/abort bên kia |
| `move ... is already on this server` / `NO_SUCH_ACCOUNT` | Guard tiền-mutation, không mất gì — kiểm tra lại src/account |
| `UNKNOWN_DOCUMENT` + hint FOSS | Dest FOSS không hiểu `MoveMailbox` → dùng `move`/`ne-move` |
| Mọi SOAP 404 `Not Found` | Servlet init chết — coi `mailbox.log` dòng `FATAL ... Unable to start servlet` (vd extension lỗi version: gỡ khỏi `lib/ext`, restart mailboxd). Chi tiết xem runbook-foss.md §6 |

Chi tiết vận hành sâu (lab 2 node, ma trận hỗ trợ, sự cố): `runbook-foss.md`.
Chi tiết từng phase kỹ thuật: `move-mailbox-steps.md`.
Đối chiếu hành vi NE: `move-mailbox-solution.md`.
