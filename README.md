có 4 loại tổng hợp theo điều kiện trên 1 trường:
tìm các bản ghi theo 1 giá trị bất kì nào đó (where field=????) ví dụ: where status=fail (trong tìm tổng số lần fail)
tìm các bản ghi theo max (where field=max...) ví dụ: where create_date=max (trong lần nạp cuối cùng)
tìm các bản ghi theo min (where field=min...)
tìm các bản ghi theo nhóm (group by) ví dụ: số lượng quảng cáo user xem theo nền tảng


Có 6 loại nhiệm vụ cần tổng hợp trên 1 trường:
tính tổng (sum), tìm max, tìm min, check tồn tại (count>0), đếm (count), lấy theo giá trị nào đó trong trường cần điều kiện (null).

 trường cần tổng hợp theo: account_id
các nhiệm vụ cần tổng hợp:
+, bảng inapp:
-, bao nhiêu lần nạp (account_id, null, count)
-, tổng đã nạp bao nhiêu tiền (account_id, null, sum(price))
-, lần cuối nạp là lúc nào (account_id, null, max(created_date))
-, lần cuối nạp bao nhiêu tiền (account_id, max(created_date), price)
-, lần nạp tối đa (account_id, null, max(price))
-, lần nạp tối thiểu (account_id, null, min(price))
+, bảng ads:
-, đã xem bao nhiêu quảng cáo (account_id, null, count)
-, số lần xem cho từng loại quảng cáo (account_id, group by(ad_where), count)
+, bảng level:
-, chơi đến level bao nhiêu (account_id, null, max(level_level))
-, tổng cộng fail bao nhiêu lần (account_id, where(status=fail), count)
+, retention:
-, lần gần nhất đăng nhập là bao giờ (account_id, null, max(created_date))
+, session:
-, lần đăng nhập gần nhất chơi bao lâu (account_id, max(created_date), sum(session_time))
-, tổng thời gian đã chơi (account_id, null, sum(session_time))
+, uninstall:
-, check xóa game hay chưa (account_id, 

-, 2 fields trong ab_testing (check lần gần nhất, nếu cả 2 bằng null thì đặt là null)
