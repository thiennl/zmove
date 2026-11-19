# Runbook: zmove trên Zimbra FOSS

Đối tượng: operator chạy `zmove` giữa các mailstore FOSS/OSE.
Mọi lệnh chạy trên **control node** (có SSH tới các mailstore).

## 1. Cài đặt

```bash
git clone <repo> /opt/zmove
ln -s /opt/zmove/bin/zmove /usr/local/bin/zmove
zmove --help
```

Không cần pip (stdlib-only, Python ≥ 3.8). Muốn cài pip: `pip install /opt/zmove`
(`console_scripts: zmove`, xem `pyproject.toml`; layout flat nên discovery
dùng `where = [".."]`).

Cấu hình (`zmove.ini`, xem `config.py` cho đủ key):

```ini
[zmove]
ssh_user = zimbra
ssh_port = 22
# node_ssh_* : SSH giữa các mailstore (rsync chạy trên source)
node_ssh_user = zimbra
node_ssh_port = 22
admin_port = 7071
state_db = ~/.local/share/zmove/state.db
```

SSH key: control → `zimbra@<src|dst>`, và `src → zimbra@<dst>`
(rsync). Kiểm tra: `ssh zimbra@<host> true` cả 3 hướng.

## 2. Lần move đầu tiên (1 account, phải làm tay)

```bash
# 0. Đọc-only, không đổi gì
zmove preflight acct@dom src dst
# 1. Chạy thật, tương tác từng bước
zmove move acct@dom -f src -t dst --confirm-steps
# 2. Verify (xem §5): count mail, login webmail cả 2 node, gửi/nhận 1 mail
# 3. Sau khi chắc chắn mới dọn bản cũ (gõ lại đúng email khi hỏi)
zmove purge acct@dom -s src
```

## 3. Batch + canary

```bash
zmove batch plan.csv --parallel 4 --per-server 2 --canary 1
```

- Canary fail → dừng, không chạy tiếp.
- Hết canary mà TTY hỏi `continue?` — trả lời `n`/EOF = pause sạch,
  `resume <job>` từng job. Non-TTY (cron) luôn dừng sau canary.
- `gc` dọn job hỏng: `zmove gc` (thủ công) hoặc timer `systemd/zmove-gc.*`
  (mặc định weekly, giữ 7 ngày).

## 4. Kẹt thì làm gì

| Tình huống | Lệnh | Ghi chú |
|---|---|---|
| Còn `LOCKED` trên source sau crash | `zmove unlock acct -s <src>` | Trả status gốc, gỡ lockout |
| Job `failed` trước `FLIPPED` | `zmove resume <job>` | Chạy tiếp từ phase đã commit |
| Muốn bỏ | `zmove abort <job>` | Chỉ được trước `FLIPPED`; xóa bản đích |
| Đã `FLIPPED`, muốn quay lại | `zmove rollback <job>` | Từ chối nếu dest đã có mail mới (trừ `--force`); xong **restart mailboxd dst** trước khi move lại |
| Đã `PURGED` | move ngược lại | Không còn đường rollback |

## 5. Checklist verify sau move

1. `zmove query <acct>` → `DONE`; `query --json` xem `snapshot` (điểm xuất phát).
2. `zmprov -l ga <acct> zimbraMailHost` == dest.
3. Login webmail trỏ vào dest OK; IMAP UID/search/tag/share còn nguyên
   (so `COUNT(*)` + UIDs mẫu trước/sau).
4. Gửi/nhận 1 mail; `postqueue -f` đã flush (tự động trừ `--no-flush-mta`).

## 6. Troubleshooting

| Lỗi | Nguyên nhân | Sửa |
|---|---|---|
| `specified destHost ...` | Sai hostname/không resolve/LDAP không có server | Sửa tên, check `zmprov -l gs` |
| `... valid mailclient server ...` | Dest thiếu service `mailbox` | `zmprov ms` / cài đủ gói |
| `admin SOAP auth failed` | Sai user/pass / 7071 chặn | Check `admin_user/password`, curl `https://dst:7071/service/admin/soap/` |
| `stale accountId->mailboxId` | JVM dst còn cache bản cũ (đã từng homed ở dst) | `ssh zimbra@dst 'zmmailboxdctl restart'` rồi preflight lại (message đã in sẵn lệnh) |
| `version/db_version/index_version differs` | 2 node khác bản Zimbra/schema | Đồng bộ bản trước, không `--force` |
| `move ... is already on this server` / `NO_SUCH_ACCOUNT` | Guard tiền-mutation của server | Kiểm tra lại src/account, không mất dữ liệu |
| `Mailbox move ...: Not Found` / `PROXY_ERROR` mọi SOAP | Servlet init chết (vd: extension lỗi version như `MailRecallExtension NoSuchFieldError`, xem `mailbox.log` `FATAL ... Unable to start servlet`) | Gỡ extension lỗi khỏi `lib/ext` (**chuyển hẳn ra ngoài**, Jetty quét mọi subdir), `zmmailboxdctl restart` |
| `ALREADY_BEING_MOVED_OUT` | Move NE khác đang giữ | Chờ/abort bên kia |
| `refusing to delete ... not owned` | Stray id thuộc account khác | Không bao giờ `--overwrite` bừa; điều tra id đó |

## 7. Ma trận hỗ trợ (trung thực)

| Chiều | Trạng thái |
|---|---|
| Python | 3.8+ stdlib-only, 90+ test (`python3 -m pytest` tại repo root) |
| Zimbra | Code path đọc schema/version động, tương thích 9.x/10.x trên lý thuyết; **E2E lab thật chưa chạy lần nào** (xem §8) — đừng prod khi chưa lab |
| MariaDB | Copy qua `mysqldump --single-transaction`; schema lạ → `discover()` từ chối explicit thay vì đoán |
| Volume | File volume: rsync+merge; external/S3: skip copy + remap locator (cần `shared_volumes`), **chưa lab** — đối soát blob mồ côi thủ công |
| NE dest | `mboxmove` (compat) dùng được; engine/`ne-move` cũng chạy được (không cần license vì không đụng API license) |
| FOSS dest | Chỉ `move`/`ne-move`; `mboxmove` sẽ báo `UNKNOWN_DOCUMENT` + hint (đúng thiết kế) |

## 8. Lab 2 node tối thiểu (bắt buộc trước prod)

Chuẩn bị 2 VM/container cùng Zimbra FOSS version, LDAP dùng chung hoặc
replica, SSH key 3 hướng (§1), 1 domain + 2 account test có sẵn mail
(>1000 msg để rsync nhiều pass).

1. `preflight` phải xanh (sửa hết error mới tiếp).
2. `move --confirm-steps`, quan sát từng phase; login webmail giữa chừng
   ở pha unlocked (phải vào được), ở pha locked (phải báo maintenance).
3. Giết controller giữa `final-sync` → `resume` phải xong; `abort` trước
   flip phải sạch (không còn stage/schema/id thừa).
4. `rollback` sau flip với mailbox chưa đổi → về src; gửi 1 mail ở dest
   rồi rollback → phải bị từ chối (trừ `--force`).
5. `purge` sai email gõ vào → từ chối; đúng email → mất bản cũ, `purges`
   ghi id để move sau tái dùng.
6. Batch 5 account `--canary 1`: canary fail (cho 1 account sai) → dừng
   đúng; canary pass + decline → còn lại skipped.

Ghi kết quả từng case vào ticket trước khi đụng mailbox thật.
