#!/usr/bin/env python3
# 驗證 NPB 每打席打點歸因 + 同局多打席解析(2026-07-12 林安可雙響 bug)
# 離線單元測試,不打網路:python3 test_npb_rbi_attribution.py
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from baseball_notifier import _attribute_npb_rbi, _extract_npb_at_bats

def check(name, actual, expected):
    status = "PASS" if actual == expected else "FAIL"
    print(f"[{status}] {name}: got={actual} expected={expected}")
    return actual == expected

ok = True

# 1. 林安可實際情境:第一轟 2 分砲(當輪唯一有打點打席)
ab1 = [("右本", True, 1)]
k1 = _attribute_npb_rbi(ab1, 2, {})
ok &= check("第一轟 2分砲", k1, {"0": 2})

# 2. 第二轟 1 分砲:總打點 2→3,增量歸因給新打席
ab2 = [("右本", True, 1), ("右中本", True, 4)]
k2 = _attribute_npb_rbi(ab2, 3, k1)
ok &= check("第二轟增量歸因 1 分", k2, {"0": 2, "1": 1})

# 3. 第三打席無打點(滾地出局),既有歸因不變
ab3 = ab2 + [("遊ゴロ", False, 6)]
k3 = _attribute_npb_rbi(ab3, 3, k2)
ok &= check("無打點打席不影響歸因", k3, {"0": 2, "1": 1})

# 4. cold start(state 遺失)同時看到 2 轟 3 打點:分不出來 → 空(顯示端 fallback 有打點)
k4 = _attribute_npb_rbi(ab2, 3, {})
ok &= check("cold start 無法均分不硬猜", k4, {})

# 5. cold start 但打席數 = 總打點:每席 1 分(對齊舊邏輯)
k5 = _attribute_npb_rbi(ab2, 2, {})
ok &= check("打席數=總打點每席1分", k5, {"0": 1, "1": 1})

# 6. 非打點打席穿插:只歸因給有打點的那席
ab6 = [("遊ゴロ", False, 1), ("右本", True, 3)]
k6 = _attribute_npb_rbi(ab6, 1, {})
ok &= check("穿插非打點打席", k6, {"1": 1})

# 7. Yahoo 下修總打點(記錄更正):歸因作廢重算
k7 = _attribute_npb_rbi(ab2, 2, {"0": 2, "1": 1})
ok &= check("總打點下修重算", k7, {"0": 1, "1": 1})

# 8. 打席的有打點標記被拿掉(記錄更正):丟掉該席歸因、重分配
ab8 = [("右本", True, 1), ("右中飛", False, 4)]  # 第二席改判無打點
k8 = _attribute_npb_rbi(ab8, 2, {"0": 2, "1": 1})
ok &= check("打點標記移除後重分配", k8, {"0": 2})

# 9. rbi_total 抓不到(=0)時不動歸因也不清空
k9 = _attribute_npb_rbi(ab2, 0, {"0": 2})
ok &= check("rbi_total=0 保留既有歸因", k9, {"0": 2})

# 10. _extract_npb_at_bats:同一局 cell 兩個結果 div(打者一巡)要全收
html = (
    '<a href="/x">林 安可</a></td>'
    + '<td>.300</td>' * 1 + '<td>4</td>' * 11
    + '<td><div class="bb-statsTable__dataDetail">遊ゴロ</div></td>'
    + '<td></td>'
    + '<td><div class="bb-statsTable__dataDetail--point bb-statsTable__dataDetail">左安</div>'
    + '<div class="bb-statsTable__dataDetail">空三振</div></td>'
    + '</tr>'
)
abx = _extract_npb_at_bats(html, ["林 安可"])
ok &= check("同局兩打席全收", abx,
            [("遊ゴロ", False, 1), ("左安", True, 3), ("空三振", False, 3)])

# 11. 一般單 div cell 行為不變
html1 = (
    '<a href="/x">林 安可</a></td>'
    + '<td>.300</td>' + '<td>4</td>' * 11
    + '<td><div class="bb-statsTable__dataDetail--point bb-statsTable__dataDetail">右本</div></td>'
    + '</tr>'
)
ab_single = _extract_npb_at_bats(html1, ["林 安可"])
ok &= check("單打席行為不變", ab_single, [("右本", True, 1)])

print("\nALL PASS" if ok else "\nSOME FAILED")
sys.exit(0 if ok else 1)
