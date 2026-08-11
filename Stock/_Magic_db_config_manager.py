#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tb_auto_config 테이블 조회 / 등록 / 수정 / 삭제 프로그램
- SQLite DB 파일을 열어 tb_auto_config 테이블의 컬럼 구조를 PRAGMA로 자동 인식합니다.
  (즉, 컬럼이 추가/변경되어도 코드 수정 없이 그대로 동작합니다)
- 메인 화면 : 리스트(Treeview)로 주요 컬럼만 표시 (컬럼이 너무 많아 전체를 표시하면 가독성이 떨어지므로)
- 행을 더블클릭하거나 '수정' 버튼을 누르면, 전체 컬럼을 스크롤 가능한 팝업 폼으로 보여주고 수정 가능
- '추가' 버튼 : 빈 폼을 띄워 신규 row INSERT
- '삭제' 버튼 : 선택된 row 삭제 (확인창 포함)

추가 기능 (정보성, 읽기 전용):
- 같은 DB 파일 안에 tb_trade 테이블이 있으면
      select tr_code, max(tr_name) tr_name from tb_trade group by tr_code
  쿼리로 ac_code(=tr_code)에 대응하는 종목명을 참고용으로 함께 보여줍니다.
- 이 값은 조회 전용이며, tb_trade 테이블에는 어떤 INSERT/UPDATE/DELETE도 하지 않습니다.
  (tb_trade 테이블이 없는 DB 파일이어도 오류 없이 정상 동작합니다)

실행 방법:
    python3 db_config_manager.py
    (실행 후 상단 '파일 열기'로 sqlite db 파일을 선택하세요)
"""

import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

TABLE_NAME = "tb_auto_config"

# 리스트 화면에 보여줄 대표 컬럼들 (전체 컬럼은 편집창에서 확인/수정)
SUMMARY_COLUMNS = [
    "ac_code",
    "ac_mode",
    "ac_max_buycount",
    "ac_buy_yn",
    "ac_buy_ordertype",
    "ac_sell_yn",
    "ac_sell_ordertype",
    "ac_pb_yn",
]

# 리스트/폼에 참고용으로 추가 표시할 종목명 컬럼 라벨
REF_NAME_COLUMN = "종목명(참고)"

# ac_code(=tr_code) -> 종목명 참고 매핑을 만들 때 사용하는 쿼리 (tb_trade, 조회 전용)
TRADE_NAME_LOOKUP_SQL = (
    "SELECT tr_code, MAX(tr_name) AS tr_name FROM tb_trade GROUP BY tr_code"
)

# 리스트 화면에 실제로 표시할 컬럼 순서 (ac_code 바로 옆에 참고용 종목명이 오도록 배치)
DISPLAY_COLUMNS = []
for _col in SUMMARY_COLUMNS:
    DISPLAY_COLUMNS.append(_col)
    if _col == "ac_code":
        DISPLAY_COLUMNS.append(REF_NAME_COLUMN)


class ColumnInfo:
    """PRAGMA table_info 결과를 담는 헬퍼"""

    def __init__(self, cid, name, ctype, notnull, dflt_value, pk):
        self.cid = cid
        self.name = name
        self.ctype = (ctype or "").upper()
        self.notnull = notnull
        self.dflt_value = dflt_value
        self.pk = pk

    @property
    def is_integer(self):
        return "INT" in self.ctype

    @property
    def is_real(self):
        return "REAL" in self.ctype or "FLOA" in self.ctype or "DOUB" in self.ctype


class RecordFormDialog(tk.Toplevel):
    """
    전체 컬럼을 보여주는 스크롤 가능한 입력/수정 폼.
    mode == 'insert' : 빈 값으로 시작, ac_code 등 직접 입력
    mode == 'update' : 기존 row 값을 채워서 시작, ac_code는 PK 성격이라 수정 불가(읽기전용) 처리
    """

    def __init__(
        self,
        master,
        columns,
        mode="insert",
        record=None,
        on_submit=None,
        copied_from=None,
        code_name_map=None,
    ):
        super().__init__(master)
        self.columns = columns
        self.mode = mode
        self.record = record or {}
        self.on_submit = on_submit
        self.entries = {}
        self.code_name_map = code_name_map or {}
        self.ref_name_var = None  # ac_code 참고용 종목명 표시

        if mode == "insert":
            title = f"행 추가 (복사 원본: {copied_from})" if copied_from else "행 추가"
        else:
            title = f"행 수정 - {self.record.get('ac_code', '')}"
        self.title(title)
        self.geometry("560x650")
        self.transient(master)
        self.grab_set()

        self._build_ui()

    def _build_ui(self):
        if self.mode == "insert" and self.record:
            # 다른 행에서 복사되어 채워진 경우 안내 문구 표시
            hint = ttk.Label(
                self,
                text="※ 선택한 행의 값이 복사되었습니다. ac_code를 새로 입력한 뒤 저장하세요.",
                foreground="blue",
            )
            hint.pack(fill="x", padx=8, pady=(6, 0))

        # 스크롤 가능한 캔버스 영역
        container = ttk.Frame(self)
        container.pack(fill="both", expand=True, padx=5, pady=5)

        canvas = tk.Canvas(container, borderwidth=0)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)

        scroll_frame.bind(
            "<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # 마우스 휠 스크롤 지원
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # 컬럼별 입력 필드 생성
        for row_idx, col in enumerate(self.columns):
            label = ttk.Label(scroll_frame, text=col.name)
            label.grid(row=row_idx, column=0, sticky="w", padx=4, pady=2)

            value = self.record.get(col.name, col.dflt_value)
            value = "" if value is None else str(value)

            entry = ttk.Entry(scroll_frame, width=30)
            entry.insert(0, value)

            # ac_code는 PK 역할 -> 수정 모드에서는 잠금 (WHERE 조건 기준 컬럼이 바뀌면 안되므로)
            if self.mode == "update" and col.name == "ac_code":
                entry.configure(state="readonly")

            entry.grid(row=row_idx, column=1, sticky="ew", padx=4, pady=2)
            self.entries[col.name] = entry

            # ac_code 필드 옆에 tb_trade 참고 종목명 표시 (읽기 전용, 정보성)
            if col.name == "ac_code":
                self.ref_name_var = tk.StringVar(value=self._lookup_name(value))
                ref_label = ttk.Label(
                    scroll_frame, textvariable=self.ref_name_var, foreground="gray"
                )
                ref_label.grid(row=row_idx, column=2, sticky="w", padx=6)
                entry.bind("<KeyRelease>", self._on_code_changed)

        scroll_frame.columnconfigure(1, weight=1)

        # 하단 버튼 영역
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", padx=5, pady=8)

        submit_text = "추가" if self.mode == "insert" else "저장"
        ttk.Button(btn_frame, text=submit_text, command=self._submit).pack(
            side="right", padx=4
        )
        ttk.Button(btn_frame, text="취소", command=self.destroy).pack(side="right")

        # 신규 추가 모드에서는 ac_code 입력란에 바로 포커스
        if self.mode == "insert" and "ac_code" in self.entries:
            self.entries["ac_code"].focus_set()

    # ---------------- ac_code 참고 종목명 (읽기 전용, tb_trade 조회) ----------------
    def _lookup_name(self, code):
        code = (code or "").strip()
        if not code:
            return ""
        name = self.code_name_map.get(code)
        if name:
            return f"참고: {name}"
        return "참고: (일치하는 종목명 없음)"

    def _on_code_changed(self, event=None):
        if self.ref_name_var is None:
            return
        code = self.entries["ac_code"].get()
        self.ref_name_var.set(self._lookup_name(code))

    def _collect_values(self):
        """입력 필드 -> {컬럼명: 파이썬 값} 딕셔너리로 변환 (타입 캐스팅 포함)"""
        result = {}
        for col in self.columns:
            raw = self.entries[col.name].get().strip()
            if raw == "":
                result[col.name] = None
                continue
            try:
                if col.is_integer:
                    result[col.name] = int(raw)
                elif col.is_real:
                    result[col.name] = float(raw)
                else:
                    result[col.name] = raw
            except ValueError:
                raise ValueError(f"'{col.name}' 컬럼 값 '{raw}' 을(를) {col.ctype} 타입으로 변환할 수 없습니다.")
        return result

    def _submit(self):
        try:
            values = self._collect_values()
        except ValueError as e:
            messagebox.showerror("입력 오류", str(e), parent=self)
            return

        if not values.get("ac_code"):
            messagebox.showerror("입력 오류", "ac_code 는 필수 값입니다.", parent=self)
            return

        if self.on_submit:
            try:
                self.on_submit(self.mode, values)
            except Exception as e:
                messagebox.showerror("DB 오류", str(e), parent=self)
                return

        self.destroy()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(f"{TABLE_NAME} 관리 프로그램")
        self.geometry("1150x520")

        self.conn = None
        self.columns = []  # ColumnInfo 리스트 (전체 컬럼)
        self.code_name_map = {}  # ac_code(=tr_code) -> 종목명 참고 매핑 (tb_trade, 조회 전용)

        self._build_ui()

    # ---------------- UI 구성 ----------------
    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill="x", padx=6, pady=6)

        ttk.Button(top, text="DB 파일 열기", command=self.open_db).pack(side="left")
        self.db_path_label = ttk.Label(top, text="(DB 파일을 열어주세요)")
        self.db_path_label.pack(side="left", padx=8)

        ttk.Button(top, text="새로고침", command=self.refresh).pack(side="right")

        # Treeview (리스트)
        tree_frame = ttk.Frame(self)
        tree_frame.pack(fill="both", expand=True, padx=6, pady=6)

        self.tree = ttk.Treeview(
            tree_frame,
            columns=DISPLAY_COLUMNS,
            show="headings",
            selectmode="browse",
        )
        for col in DISPLAY_COLUMNS:
            self.tree.heading(col, text=col)
            if col == REF_NAME_COLUMN:
                self.tree.column(col, width=130, anchor="w")
            else:
                self.tree.column(col, width=100, anchor="center")

        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        self.tree.bind("<Double-1>", lambda e: self.edit_selected())

        # 하단 버튼
        bottom = ttk.Frame(self)
        bottom.pack(fill="x", padx=6, pady=6)

        ttk.Button(bottom, text="추가", command=self.insert_new).pack(side="left")
        ttk.Label(
            bottom, text="(행을 선택 후 [추가]를 누르면 값이 복사됩니다)", foreground="gray"
        ).pack(side="left", padx=6)
        ttk.Button(bottom, text="수정", command=self.edit_selected).pack(side="left", padx=4)
        ttk.Button(bottom, text="삭제", command=self.delete_selected).pack(side="left")

        self.status_label = ttk.Label(bottom, text="")
        self.status_label.pack(side="right")

    # ---------------- DB 연결 / 조회 ----------------
    def open_db(self):
        path = filedialog.askopenfilename(
            title="SQLite DB 파일 선택",
            filetypes=[("SQLite DB", "*.db *.sqlite *.sqlite3"), ("모든 파일", "*.*")],
        )
        if not path:
            return

        try:
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            cur = conn.execute(f"PRAGMA table_info({TABLE_NAME})")
            rows = cur.fetchall()
            if not rows:
                messagebox.showerror(
                    "오류", f"'{TABLE_NAME}' 테이블을 찾을 수 없습니다."
                )
                conn.close()
                return
        except sqlite3.Error as e:
            messagebox.showerror("DB 오류", str(e))
            return

        self.conn = conn
        self.columns = [
            ColumnInfo(r["cid"], r["name"], r["type"], r["notnull"], r["dflt_value"], r["pk"])
            for r in rows
        ]
        self.db_path_label.configure(text=path)
        self._refresh_code_name_map()
        self.refresh()

    def _refresh_code_name_map(self):
        """ac_code(=tr_code) -> 종목명 참고 매핑 갱신 (tb_trade, 조회 전용).
        tb_trade 테이블이 없는 DB 파일이면 조용히 빈 매핑으로 둠(오류 없음)."""
        self.code_name_map = {}
        if not self.conn:
            return
        try:
            cur = self.conn.execute(TRADE_NAME_LOOKUP_SQL)
            for code, name in cur.fetchall():
                if code is not None:
                    self.code_name_map[code] = name
        except sqlite3.Error:
            self.code_name_map = {}

    def refresh(self):
        if not self.conn:
            return
        for item in self.tree.get_children():
            self.tree.delete(item)

        self._refresh_code_name_map()

        cur = self.conn.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY ac_code")
        rows = cur.fetchall()
        for r in rows:
            ref_name = self.code_name_map.get(r["ac_code"], "")
            values = []
            for col in DISPLAY_COLUMNS:
                if col == REF_NAME_COLUMN:
                    values.append(ref_name)
                else:
                    values.append(r[col] if col in r.keys() else "")
            # ac_code 를 iid로 사용해서 선택 시 바로 조회 가능하게 함
            self.tree.insert("", "end", iid=str(r["ac_code"]), values=values)

        self.status_label.configure(text=f"총 {len(rows)} 건")

    def _get_selected_record(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("안내", "먼저 행을 선택하세요.")
            return None
        ac_code = sel[0]
        cur = self.conn.execute(
            f"SELECT * FROM {TABLE_NAME} WHERE ac_code = ?", (ac_code,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return dict(row)

    # ---------------- 추가 / 수정 / 삭제 ----------------
    def insert_new(self):
        if not self.conn:
            messagebox.showinfo("안내", "먼저 DB 파일을 열어주세요.")
            return

        # 선택된 행이 있으면 그 값을 복사해서 신규 입력 폼에 채워줌 (ac_code만 비움)
        base_record = None
        source_code = None
        sel = self.tree.selection()
        if sel:
            base_record = self._get_selected_record()
            if base_record is not None:
                base_record = dict(base_record)
                source_code = base_record.get("ac_code")
                base_record["ac_code"] = ""  # 새 코드는 직접 입력해야 하므로 비움

        RecordFormDialog(
            self,
            self.columns,
            mode="insert",
            record=base_record,
            on_submit=self._handle_submit,
            copied_from=source_code,
            code_name_map=self.code_name_map,
        )

    def edit_selected(self):
        if not self.conn:
            messagebox.showinfo("안내", "먼저 DB 파일을 열어주세요.")
            return
        record = self._get_selected_record()
        if record is None:
            return
        RecordFormDialog(
            self,
            self.columns,
            mode="update",
            record=record,
            on_submit=self._handle_submit,
            code_name_map=self.code_name_map,
        )

    def delete_selected(self):
        if not self.conn:
            messagebox.showinfo("안내", "먼저 DB 파일을 열어주세요.")
            return
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("안내", "먼저 삭제할 행을 선택하세요.")
            return
        ac_code = sel[0]
        if not messagebox.askyesno(
            "삭제 확인", f"ac_code = '{ac_code}' 행을 삭제하시겠습니까?"
        ):
            return
        try:
            self.conn.execute(f"DELETE FROM {TABLE_NAME} WHERE ac_code = ?", (ac_code,))
            self.conn.commit()
        except sqlite3.Error as e:
            messagebox.showerror("DB 오류", str(e))
            return
        self.refresh()

    def _handle_submit(self, mode, values):
        """RecordFormDialog 에서 추가/수정 확정 시 호출되는 콜백"""
        col_names = [c.name for c in self.columns]

        if mode == "insert":
            # 이미 존재하는 ac_code 인지 체크
            cur = self.conn.execute(
                f"SELECT 1 FROM {TABLE_NAME} WHERE ac_code = ?", (values["ac_code"],)
            )
            if cur.fetchone():
                raise Exception(f"ac_code '{values['ac_code']}' 는 이미 존재합니다.")

            placeholders = ", ".join(["?"] * len(col_names))
            col_list = ", ".join(col_names)
            params = [values.get(c) for c in col_names]
            self.conn.execute(
                f"INSERT INTO {TABLE_NAME} ({col_list}) VALUES ({placeholders})", params
            )
        else:
            # ac_code 는 WHERE 조건이므로 SET 절에서 제외
            set_cols = [c for c in col_names if c != "ac_code"]
            set_clause = ", ".join([f"{c} = ?" for c in set_cols])
            params = [values.get(c) for c in set_cols]
            params.append(values["ac_code"])
            self.conn.execute(
                f"UPDATE {TABLE_NAME} SET {set_clause} WHERE ac_code = ?", params
            )

        self.conn.commit()
        self.refresh()

    def destroy(self):
        if self.conn:
            self.conn.close()
        super().destroy()


if __name__ == "__main__":
    app = App()
    app.mainloop()
