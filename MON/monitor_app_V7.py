import os
import re
import bisect
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime
import matplotlib
matplotlib.use('TkAgg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.dates as mdates

# 윈도우 한글 폰트 깨짐 방지 및 마이너스 기호 설정
matplotlib.rcParams['font.family'] = 'Malgun Gothic'
matplotlib.rcParams['axes.unicode_minus'] = False

# 다양한 년월일시분초 형식 파싱용 정규표현식
# - 유형1: 20261127151735
# - 유형2: 2026-11-27-15.17.35 / 2026-11-27 15:17:35 (공백/특수문자 포함)
# - 유형3: 2026-11-27-15.17.35.345782 (초 뒤 추가 숫자/소수부 무시)
TS_PATTERN = re.compile(
    r'^\s*'
    r'(\d{4})[^\d\r\n]{0,3}(\d{2})[^\d\r\n]{0,3}(\d{2})'
    r'[^\d\r\n]{0,3}(\d{2})[^\d\r\n]{0,3}(\d{2})[^\d\r\n]{0,3}(\d{2})'
    r'\S*\s+(.+)$'
)

# 축별 다중 데이터 구분용 색상 팔레트: (MA 추세선 색, Raw 데이터 색)
AXIS1_COLORS = [
    ('#0000FF', '#1E90FF'),  # 파랑 / 도저블루
    ('#000080', '#4169E1'),  # 네이비 / 로열블루
    ('#008080', '#20B2AA'),  # 틸 / 라이트시그린
    ('#4B0082', '#9370DB'),  # 인디고 / 미디엄퍼플
    ('#006400', '#3CB371'),  # 다크그린 / 미디엄시그린
    ('#008B8B', '#48D1CC'),  # 다크시안 / 미디엄터쿼이즈
]

AXIS2_COLORS = [
    ('#FF0000', '#FA8072'),  # 빨강 / 연어색
    ('#D2691E', '#FFA07A'),  # 초콜릿 / 라이트연어
    ('#C71585', '#FF69B4'),  # 바이올렛레드 / 핫핑크
    ('#B8860B', '#DAA520'),  # 다크골든로드 / 골든로드
    ('#8B0000', '#CD5C5C'),  # 다크레드 / 인디안레드
    ('#FF4500', '#FF7F50'),  # 오렌지레드 / 코랄
]


class DataVisualizerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DB 성능 로그 이중 축 분석기 Pro v7")
        self.root.geometry("1280x900")

        # 다중 파일 경로 리스트 (인덱스 0이 커서 표시 및 자동 포개기 기준)
        self.files1 = []
        self.files2 = []
        self.ax2 = None

        # 파싱 및 변환 완료된 데이터 캐시 리스트
        self.series1 = []
        self.series2 = []
        self.ma_window = 60
        self.base_y2_max = 100.0
        self.vline = None
        self._resize_job = None
        self._closing = False
        self._mpl_cids = []
        self.controls_visible = True

        # 축별 원본 데이터 유형 ('delta', 'delta_rate', 'cum_diff', 'cum_rate')
        self.mode1_var = tk.StringVar(value="delta")
        self.mode2_var = tk.StringVar(value="delta")

        # ==================================================================
        # 1. 접기/펼치기 가능한 상단 컨트롤 컨테이너
        # ==================================================================
        self.ctrl_container = tk.Frame(self.root)
        self.ctrl_container.pack(fill=tk.X, padx=6, pady=(3, 1))

        # --- 1-1. 축1 / 축2 파일 및 데이터 유형 선택 (슬림 1행 컨트롤 + 2행 리스트) ---
        top_frame = tk.Frame(self.ctrl_container)
        top_frame.pack(fill=tk.X, pady=(0, 2))

        # [축 1 패널]
        axis1_frame = tk.LabelFrame(
            top_frame, text="📁 축 1 (좌측 Y축 - 파랑 계열 / [1★] 기준)",
            fg="#003399", font=("", 8, "bold"), padx=4, pady=2
        )
        axis1_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 4))

        a1_bar = tk.Frame(axis1_frame)
        a1_bar.pack(fill=tk.X, pady=(0, 2))
        tk.Button(a1_bar, text="➕추가", command=self.add_files1, width=6, padx=1).pack(side=tk.LEFT, padx=1)
        tk.Button(a1_bar, text="➖삭제", command=self.remove_selected1, width=6, padx=1).pack(side=tk.LEFT, padx=1)
        tk.Button(a1_bar, text="❌비움", command=self.clear_files1, width=6, padx=1).pack(side=tk.LEFT, padx=1)
        tk.Button(a1_bar, text="▲", command=lambda: self.move_item(1, -1), width=2).pack(side=tk.LEFT, padx=(3, 0))
        tk.Button(a1_bar, text="▼", command=lambda: self.move_item(1, 1), width=2).pack(side=tk.LEFT, padx=1)

        tk.Label(a1_bar, text="|", fg="gray").pack(side=tk.LEFT, padx=3)
        tk.Radiobutton(a1_bar, text="Delta", variable=self.mode1_var, value="delta", command=self.on_mode_change).pack(side=tk.LEFT)
        tk.Radiobutton(a1_bar, text="Delta(/s)", variable=self.mode1_var, value="delta_rate", command=self.on_mode_change).pack(side=tk.LEFT)
        tk.Radiobutton(a1_bar, text="누적(차분)", variable=self.mode1_var, value="cum_diff", command=self.on_mode_change).pack(side=tk.LEFT)
        tk.Radiobutton(a1_bar, text="누적(/s)", variable=self.mode1_var, value="cum_rate", command=self.on_mode_change).pack(side=tk.LEFT)

        a1_list_row = tk.Frame(axis1_frame)
        a1_list_row.pack(fill=tk.BOTH, expand=True)
        self.listbox1 = tk.Listbox(a1_list_row, height=2, selectmode=tk.EXTENDED, font=("Consolas", 9))
        sb1 = tk.Scrollbar(a1_list_row, orient=tk.VERTICAL, command=self.listbox1.yview)
        self.listbox1.config(yscrollcommand=sb1.set)
        self.listbox1.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb1.pack(side=tk.RIGHT, fill=tk.Y)

        # [축 2 패널]
        axis2_frame = tk.LabelFrame(
            top_frame, text="📁 축 2 (우측 Y축 - 빨강 계열 / 선택 사항)",
            fg="#990000", font=("", 8, "bold"), padx=4, pady=2
        )
        axis2_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))

        a2_bar = tk.Frame(axis2_frame)
        a2_bar.pack(fill=tk.X, pady=(0, 2))
        tk.Button(a2_bar, text="➕추가", command=self.add_files2, width=6, padx=1).pack(side=tk.LEFT, padx=1)
        tk.Button(a2_bar, text="➖삭제", command=self.remove_selected2, width=6, padx=1).pack(side=tk.LEFT, padx=1)
        tk.Button(a2_bar, text="❌비움", command=self.clear_files2, width=6, padx=1).pack(side=tk.LEFT, padx=1)
        tk.Button(a2_bar, text="▲", command=lambda: self.move_item(2, -1), width=2).pack(side=tk.LEFT, padx=(3, 0))
        tk.Button(a2_bar, text="▼", command=lambda: self.move_item(2, 1), width=2).pack(side=tk.LEFT, padx=1)

        tk.Label(a2_bar, text="|", fg="gray").pack(side=tk.LEFT, padx=3)
        tk.Radiobutton(a2_bar, text="Delta", variable=self.mode2_var, value="delta", command=self.on_mode_change).pack(side=tk.LEFT)
        tk.Radiobutton(a2_bar, text="Delta(/s)", variable=self.mode2_var, value="delta_rate", command=self.on_mode_change).pack(side=tk.LEFT)
        tk.Radiobutton(a2_bar, text="누적(차분)", variable=self.mode2_var, value="cum_diff", command=self.on_mode_change).pack(side=tk.LEFT)
        tk.Radiobutton(a2_bar, text="누적(/s)", variable=self.mode2_var, value="cum_rate", command=self.on_mode_change).pack(side=tk.LEFT)

        a2_list_row = tk.Frame(axis2_frame)
        a2_list_row.pack(fill=tk.BOTH, expand=True)
        self.listbox2 = tk.Listbox(a2_list_row, height=2, selectmode=tk.EXTENDED, font=("Consolas", 9))
        sb2 = tk.Scrollbar(a2_list_row, orient=tk.VERTICAL, command=self.listbox2.yview)
        self.listbox2.config(yscrollcommand=sb2.set)
        self.listbox2.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb2.pack(side=tk.RIGHT, fill=tk.Y)

        # [그래프 그리기 버튼]
        self.btn_plot = tk.Button(
            top_frame, text="📊\n그래프\n그리기", command=self.plot_data,
            bg="#cfe8ff", font=("", 9, "bold"), width=8
        )
        self.btn_plot.pack(side=tk.RIGHT, fill=tk.Y, pady=2)

        # --- 1-2. Y축 범위 / MA / 표시 옵션 + 축2 위치/배율 슬림 제어 바 ---
        opt_move_frame = tk.Frame(self.ctrl_container, bg="#fafafa", bd=1, relief="groove")
        opt_move_frame.pack(fill=tk.X, pady=(1, 1))

        # 상단 서브라인: Y축 수동 설정 & MA & 표시 체크박스
        opt_row = tk.Frame(opt_move_frame, bg="#fafafa")
        opt_row.pack(fill=tk.X, padx=4, pady=1)

        tk.Label(opt_row, text="🔍 좌Y최대:", bg="#fafafa").pack(side=tk.LEFT, padx=(2, 1))
        self.y1_entry = tk.Entry(opt_row, width=7)
        self.y1_entry.pack(side=tk.LEFT, padx=2)

        tk.Label(opt_row, text="우Y최대:", bg="#fafafa").pack(side=tk.LEFT, padx=(4, 1))
        self.y2_entry = tk.Entry(opt_row, width=7)
        self.y2_entry.pack(side=tk.LEFT, padx=2)

        tk.Button(opt_row, text="적용", command=self.apply_y_limit, padx=5).pack(side=tk.LEFT, padx=2)
        tk.Button(opt_row, text="원래대로", command=self.reset_view, padx=5).pack(side=tk.LEFT, padx=2)

        tk.Label(opt_row, text=" | 📈 MA:", bg="#fafafa").pack(side=tk.LEFT, padx=(4, 1))
        self.ma_entry = tk.Entry(opt_row, width=5)
        self.ma_entry.insert(0, "60")
        self.ma_entry.pack(side=tk.LEFT, padx=2)

        self.show_raw_var = tk.BooleanVar(value=True)
        tk.Checkbutton(
            opt_row, text="Raw 표시", variable=self.show_raw_var, bg="#fafafa", command=self.plot_data
        ).pack(side=tk.LEFT, padx=4)

        self.show_peak_var = tk.BooleanVar(value=True)
        tk.Checkbutton(
            opt_row, text="최고점(Peak)", variable=self.show_peak_var, bg="#fafafa", command=self.plot_data
        ).pack(side=tk.LEFT, padx=4)

        # 같은 줄 우측에 축2 위치/배율 슬라이더 배치
        tk.Label(opt_row, text=" | ", bg="#fafafa", fg="gray").pack(side=tk.LEFT, padx=2)
        tk.Label(opt_row, text="🎛️ 축2 ↕이동:", bg="#fafafa", fg="#b00000", font=("", 8, "bold")).pack(side=tk.LEFT, padx=(2, 1))
        self.offset_scale = tk.Scale(
            opt_row, from_=-100, to=100, orient=tk.HORIZONTAL, length=130, width=10,
            sliderlength=14, resolution=1, bg="#fafafa", highlightthickness=0,
            font=("", 7), command=self.update_ax2_transform
        )
        self.offset_scale.set(0)
        self.offset_scale.pack(side=tk.LEFT, padx=2)

        tk.Label(opt_row, text="🔍배율:", bg="#fafafa", fg="#b00000", font=("", 8, "bold")).pack(side=tk.LEFT, padx=(4, 1))
        self.zoom_scale = tk.Scale(
            opt_row, from_=0.2, to=10.0, orient=tk.HORIZONTAL, length=130, width=10,
            sliderlength=14, resolution=0.1, bg="#fafafa", highlightthickness=0,
            font=("", 7), command=self.update_ax2_transform
        )
        self.zoom_scale.set(1.0)
        self.zoom_scale.pack(side=tk.LEFT, padx=2)

        tk.Label(
            opt_row, text="(Shift+휠:배율 / Ctrl+휠:이동)",
            bg="#fafafa", fg="#777777", font=("", 8)
        ).pack(side=tk.RIGHT, padx=4)

        self.y1_entry.bind('<Return>', lambda event: self.apply_y_limit())
        self.y2_entry.bind('<Return>', lambda event: self.apply_y_limit())
        self.ma_entry.bind('<Return>', lambda event: self.plot_data())

        # ==================================================================
        # 2. 상시 표시 바 (접기/펼치기 토글 + Matplotlib 툴바 + 빠른 버튼)
        # ==================================================================
        self.bar_frame = tk.Frame(self.root, bg="#ececec", bd=1, relief="raised")
        self.bar_frame.pack(fill=tk.X, padx=6, pady=1)

        self.btn_toggle_ctrl = tk.Button(
            self.bar_frame, text="▲ 설정 접기 (F2)", command=self.toggle_controls,
            bg="#dfe7f2", fg="#113366", font=("", 9, "bold"), padx=8, pady=2
        )
        self.btn_toggle_ctrl.pack(side=tk.LEFT, padx=(3, 6), pady=2)

        # Matplotlib 캔버스 생성 (pyplot 대신 Figure 직접 생성으로 종료 시 프로세스 잔류 방지)
        self.fig = Figure()
        self.ax1 = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.root)

        toolbar_holder = tk.Frame(self.bar_frame, bg="#ececec")
        toolbar_holder.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.toolbar = NavigationToolbar2Tk(self.canvas, toolbar_holder)
        self.toolbar.update()

        # 접은 상태에서도 자주 쓰는 빠른 액션 버튼
        tk.Button(
            self.bar_frame, text="🔄 초기화", command=self.reset_view,
            bg="#ffffff", font=("", 8), padx=6, pady=2
        ).pack(side=tk.RIGHT, padx=(2, 4), pady=2)

        tk.Button(
            self.bar_frame, text="✨ 추세선 자동 포개기", command=self.auto_align_ax2,
            bg="#ffebeb", fg="#900000", font=("", 8, "bold"), padx=6, pady=2
        ).pack(side=tk.RIGHT, padx=2, pady=2)

        # ==================================================================
        # 3. 실시간 커서 정보 및 통계 요약 바 (상시 표시, 컴팩트)
        # ==================================================================
        self.info_frame = tk.Frame(self.root, bg="#f0f4f8", bd=1, relief="solid")
        self.info_frame.pack(fill=tk.X, padx=6, pady=(1, 2))

        self.lbl_stats = tk.Label(
            self.info_frame, text="💡 각 축에 파일을 추가하고 [그래프 그리기]를 눌러주세요.",
            bg="#f0f4f8", font=("Consolas", 9), anchor="w"
        )
        self.lbl_stats.pack(fill=tk.X, padx=6, pady=(1, 0))

        self.lbl_cursor = tk.Label(
            self.info_frame, text="🎯 마우스 커서 위치 (각 축 1번째 데이터 기준): -",
            bg="#f0f4f8", fg="#222222", font=("Consolas", 9, "bold"), anchor="w"
        )
        self.lbl_cursor.pack(fill=tk.X, padx=6, pady=(0, 1))

        # ==================================================================
        # 4. 그래프 캔버스 (남은 화면 100% 확장)
        # ==================================================================
        canvas_widget = self.canvas.get_tk_widget()
        canvas_widget.pack(fill=tk.BOTH, expand=True, padx=0, pady=0)

        # Matplotlib 공식 resize_event 사용 (Tk <Configure> 덮어쓰기 방지 -> 전체 화면 꽉 채움)
        self._mpl_cids = [
            self.canvas.mpl_connect('resize_event', self.on_fig_resize),
            self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move),
            self.canvas.mpl_connect('scroll_event', self.on_mouse_scroll),
        ]

        # 단축키 F2: 상단 설정 패널 접기/펼치기
        self.root.bind('<F2>', lambda _e: self.toggle_controls())

        # 창 X 버튼 클릭 시 깔끔한 종료 처리 등록
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        self.adjust_layout(draw=False)

    # ------------------------------------------------------------------
    # 안전한 프로그램 종료 처리
    # ------------------------------------------------------------------
    def on_closing(self):
        """창 X 버튼 클릭 시 예약 작업과 Matplotlib 리소스를 정리하고 완전히 종료"""
        if self._closing:
            return
        self._closing = True

        if self._resize_job is not None:
            try:
                self.root.after_cancel(self._resize_job)
            except Exception:
                pass
            self._resize_job = None

        for cid in self._mpl_cids:
            try:
                self.canvas.mpl_disconnect(cid)
            except Exception:
                pass
        self._mpl_cids.clear()

        try:
            self.fig.clear()
        except Exception:
            pass

        try:
            self.root.quit()
        finally:
            self.root.destroy()

    # ------------------------------------------------------------------
    # 설정 영역 접기/펼치기 및 그래프 여백 최소화
    # ------------------------------------------------------------------
    def toggle_controls(self):
        """상단 컨트롤 패널을 접거나 펼쳐 그래프 영역을 최대화"""
        if self.controls_visible:
            self.ctrl_container.pack_forget()
            self.controls_visible = False
            self.btn_toggle_ctrl.config(text="▼ 설정 펼치기 (F2)", bg="#ffe8cc", fg="#884400")
        else:
            self.ctrl_container.pack(fill=tk.X, padx=6, pady=(3, 1), before=self.bar_frame)
            self.controls_visible = True
            self.btn_toggle_ctrl.config(text="▲ 설정 접기 (F2)", bg="#dfe7f2", fg="#113366")

        self.root.update_idletasks()
        self.schedule_layout_adjust()

    def adjust_layout(self, draw=True):
        """축 라벨과 제목을 가리지 않는 최소 공백으로 그래프를 꽉 채움"""
        self._resize_job = None
        if self._closing:
            return
        try:
            self.fig.tight_layout(pad=0.3)
        except Exception:
            pass
        if draw:
            self.canvas.draw_idle()

    def schedule_layout_adjust(self):
        if self._closing:
            return
        if self._resize_job is not None:
            self.root.after_cancel(self._resize_job)
        self._resize_job = self.root.after(80, self.adjust_layout)

    def on_fig_resize(self, _event):
        """Matplotlib가 새 창 크기에 맞춰 캔버스를 리사이즈한 직후 여백 재계산"""
        if self._closing:
            return
        self.schedule_layout_adjust()

    # ------------------------------------------------------------------
    # 파일 목록 관리
    # ------------------------------------------------------------------
    def _refresh_listbox(self, listbox, file_list):
        listbox.delete(0, tk.END)
        for idx, path in enumerate(file_list):
            prefix = "[1★] " if idx == 0 else f"[{idx + 1}]  "
            listbox.insert(tk.END, prefix + os.path.basename(path))

    def move_item(self, axis_num, direction):
        """선택한 항목의 순서를 위(-1) / 아래(+1)로 이동하여 [1★] 기준 변경"""
        listbox = self.listbox1 if axis_num == 1 else self.listbox2
        file_list = self.files1 if axis_num == 1 else self.files2

        sel = listbox.curselection()
        if len(sel) != 1:
            return
        idx = sel[0]
        new_idx = idx + direction
        if 0 <= new_idx < len(file_list):
            file_list[idx], file_list[new_idx] = file_list[new_idx], file_list[idx]
            self._refresh_listbox(listbox, file_list)
            listbox.selection_set(new_idx)
            listbox.see(new_idx)
            if self.files1 and self.series1:
                self.plot_data()

    def add_files1(self):
        paths = filedialog.askopenfilenames(
            title="축 1 (왼쪽 Y축) 파일 선택 (다중 선택 가능)",
            filetypes=[("Text/Log files", "*.txt *.log *.csv"), ("All files", "*.*")]
        )
        if paths:
            for p in paths:
                if p not in self.files1:
                    self.files1.append(p)
            self._refresh_listbox(self.listbox1, self.files1)

    def remove_selected1(self):
        selected = list(self.listbox1.curselection())
        for idx in reversed(selected):
            del self.files1[idx]
        self._refresh_listbox(self.listbox1, self.files1)
        if self.files1 and self.series1:
            self.plot_data()

    def clear_files1(self):
        self.files1.clear()
        self._refresh_listbox(self.listbox1, self.files1)

    def add_files2(self):
        paths = filedialog.askopenfilenames(
            title="축 2 (오른쪽 Y축) 파일 선택 (다중 선택 가능)",
            filetypes=[("Text/Log files", "*.txt *.log *.csv"), ("All files", "*.*")]
        )
        if paths:
            for p in paths:
                if p not in self.files2:
                    self.files2.append(p)
            self._refresh_listbox(self.listbox2, self.files2)

    def remove_selected2(self):
        selected = list(self.listbox2.curselection())
        for idx in reversed(selected):
            del self.files2[idx]
        self._refresh_listbox(self.listbox2, self.files2)
        if self.files1 and self.series1:
            self.plot_data()

    def clear_files2(self):
        self.files2.clear()
        self._refresh_listbox(self.listbox2, self.files2)
        if self.files1 and self.series1:
            self.plot_data()

    def on_mode_change(self):
        """데이터 유형(Delta / Delta(/s) / 누적차분 / 누적초당) 변경 시 그래프 즉시 재갱신"""
        if self.files1 and self.series1:
            self.plot_data()

    # ------------------------------------------------------------------
    # 데이터 파싱 및 변환
    # ------------------------------------------------------------------
    def _get_mode_meta(self, mode):
        if mode == "delta_rate":
            return " [Delta /s]", "/s"
        if mode == "cum_diff":
            return " [누적차분]", ""
        if mode == "cum_rate":
            return " [누적 /s]", "/s"
        return "", ""

    def parse_file(self, file_path):
        records = []
        target_name = ""
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                m = TS_PATTERN.match(line)
                if not m:
                    continue
                try:
                    y, mo, d, h, mi, s, rest = m.groups()
                    dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(s))
                    parts = rest.split()
                    if len(parts) >= 2:
                        if not target_name:
                            target_name = parts[0]
                        records.append((dt, float(parts[1])))
                except ValueError:
                    continue

        if not target_name:
            target_name = os.path.splitext(os.path.basename(file_path))[0]

        records.sort(key=lambda x: x[0])
        times = [r[0] for r in records]
        values = [r[1] for r in records]
        return times, values, target_name

    def apply_value_mode(self, times, values, mode):
        """
        delta      : 원본 값 그대로 반환
        delta_rate : 원본 값 / 경과초 (경과초 <= 0 구간은 스킵)
        cum_diff   : 현재값 - 이전값 (카운터 리셋으로 인한 음수는 0.0 보정)
        cum_rate   : (현재값 - 이전값) / 경과초 (경과초 <= 0 구간은 스킵)
        """
        if mode == "delta":
            return times, values

        if len(values) < 2:
            return [], []

        out_times = []
        out_vals = []
        for i in range(1, len(values)):
            if mode == "delta_rate":
                dt_sec = (times[i] - times[i - 1]).total_seconds()
                if dt_sec <= 0:
                    continue
                val = values[i] / dt_sec
            else:
                diff = values[i] - values[i - 1]
                if diff < 0:
                    diff = 0.0

                if mode == "cum_rate":
                    dt_sec = (times[i] - times[i - 1]).total_seconds()
                    if dt_sec <= 0:
                        continue
                    val = diff / dt_sec
                else:
                    val = diff

            out_times.append(times[i])
            out_vals.append(val)

        return out_times, out_vals

    def get_moving_average(self, values, window_size=60):
        ma = []
        current_sum = 0.0
        for i, val in enumerate(values):
            current_sum += val
            if i >= window_size:
                current_sum -= values[i - window_size]
                ma.append(current_sum / window_size)
            else:
                ma.append(current_sum / (i + 1))
        return ma

    def load_axis_series(self, file_paths, mode, ma_window):
        """특정 축의 모든 파일을 로드하고 동일 모드 및 MA 계산 적용"""
        raw_list = []
        name_counts = {}
        _, unit = self._get_mode_meta(mode)

        for path in file_paths:
            times, vals, name = self.parse_file(path)
            times, vals = self.apply_value_mode(times, vals, mode)
            if not times:
                raise ValueError(
                    f"유효한 데이터를 읽지 못했습니다 (차분/초당 모드는 서로 다른 시각의 2행 이상 필요):\n{os.path.basename(path)}"
                )
            raw_list.append((path, name, times, vals))
            name_counts[name] = name_counts.get(name, 0) + 1

        series_list = []
        for path, name, times, vals in raw_list:
            display_name = (
                f"{name}({os.path.splitext(os.path.basename(path))[0]})"
                if name_counts[name] > 1 else name
            )
            ma = self.get_moving_average(vals, window_size=ma_window)
            series_list.append({
                'name': display_name,
                'times': times,
                'vals': vals,
                'ma': ma,
                'unit': unit,
                'path': path
            })
        return series_list

    def format_stat(self, series, total_count):
        vals = series['vals']
        times = series['times']
        unit = series['unit']
        if not vals:
            return ""
        max_v = max(vals)
        min_v = min(vals)
        avg_v = sum(vals) / len(vals)
        max_t = times[vals.index(max_v)].strftime('%H:%M:%S')
        extra = f" (외 {total_count - 1}건)" if total_count > 1 else ""
        return (
            f"[{series['name']}{extra}] "
            f"Max: {max_v:,.1f}{unit} ({max_t}) | Avg: {avg_v:,.1f}{unit} | Min: {min_v:,.1f}{unit}"
        )

    # ------------------------------------------------------------------
    # 그래프 그리기
    # ------------------------------------------------------------------
    def plot_data(self):
        if not self.files1:
            messagebox.showwarning("경고", "축 1(왼쪽 Y축) 파일을 1개 이상 추가해주세요.")
            return

        try:
            self.ma_window = max(1, int(self.ma_entry.get().strip()))
        except ValueError:
            self.ma_window = 60
            self.ma_entry.delete(0, tk.END)
            self.ma_entry.insert(0, "60")

        self.ax1.clear()
        if self.ax2:
            self.ax2.remove()
            self.ax2 = None
        self.vline = None

        self.offset_scale.set(0)
        self.zoom_scale.set(1.0)

        try:
            mode1 = self.mode1_var.get()
            mode2 = self.mode2_var.get()
            mode1_tag, _ = self._get_mode_meta(mode1)
            mode2_tag, _ = self._get_mode_meta(mode2)

            # --- 1. 축 1 (왼쪽 Y축) ---
            self.series1 = self.load_axis_series(self.files1, mode1, self.ma_window)
            min_y1 = float('inf')

            for idx, s in enumerate(self.series1):
                ma_color, raw_color = AXIS1_COLORS[idx % len(AXIS1_COLORS)]
                min_y1 = min(min_y1, min(s['vals']))

                if self.show_raw_var.get():
                    self.ax1.plot(
                        s['times'], s['vals'], color=raw_color,
                        linewidth=0.5, alpha=0.40, label=f"{s['name']} Raw"
                    )
                self.ax1.plot(
                    s['times'], s['ma'], color=ma_color,
                    linewidth=1.6, alpha=0.95, label=f"{s['name']} {self.ma_window} MA"
                )
                if self.show_peak_var.get():
                    max_v = max(s['vals'])
                    max_t = s['times'][s['vals'].index(max_v)]
                    self.ax1.scatter(
                        [max_t], [max_v], color=ma_color, s=36, zorder=5,
                        label=f"{s['name']} Peak ({max_v:,.1f}{s['unit']})"
                    )

            y1_label = (
                self.series1[0]['name']
                if len(self.series1) == 1
                else f"{self.series1[0]['name']} 외 {len(self.series1) - 1}건"
            )
            self.ax1.set_xlabel("Time (HH:MM:SS)", fontsize=9, labelpad=2)
            self.ax1.set_ylabel(y1_label + mode1_tag, fontsize=10, color='blue', fontweight='bold', labelpad=3)
            self.ax1.tick_params(axis='y', labelcolor='blue', labelsize=9)
            self.ax1.margins(x=0, y=0.03)
            if min_y1 >= 0:
                self.ax1.set_ylim(bottom=0)

            stat_text = self.format_stat(self.series1[0], len(self.series1))

            # --- 2. 축 2 (오른쪽 Y축) ---
            if self.files2:
                self.series2 = self.load_axis_series(self.files2, mode2, self.ma_window)
                self.ax2 = self.ax1.twinx()
                min_y2 = float('inf')

                for idx, s in enumerate(self.series2):
                    ma_color, raw_color = AXIS2_COLORS[idx % len(AXIS2_COLORS)]
                    min_y2 = min(min_y2, min(s['vals']))

                    if self.show_raw_var.get():
                        self.ax2.plot(
                            s['times'], s['vals'], color=raw_color,
                            linewidth=0.5, alpha=0.40, label=f"{s['name']} Raw"
                        )
                    self.ax2.plot(
                        s['times'], s['ma'], color=ma_color,
                        linewidth=1.6, alpha=0.95, label=f"{s['name']} {self.ma_window} MA"
                    )
                    if self.show_peak_var.get():
                        max_v = max(s['vals'])
                        max_t = s['times'][s['vals'].index(max_v)]
                        self.ax2.scatter(
                            [max_t], [max_v], color=ma_color, s=36, zorder=5,
                            label=f"{s['name']} Peak ({max_v:,.1f}{s['unit']})"
                        )

                y2_label = (
                    self.series2[0]['name']
                    if len(self.series2) == 1
                    else f"{self.series2[0]['name']} 외 {len(self.series2) - 1}건"
                )
                self.ax2.set_ylabel(y2_label + mode2_tag, fontsize=10, color='red', fontweight='bold', labelpad=3)
                self.ax2.tick_params(axis='y', labelcolor='red', labelsize=9)
                self.ax2.margins(x=0, y=0.03)
                if min_y2 >= 0:
                    self.ax2.set_ylim(bottom=0)

                _, top2 = self.ax2.get_ylim()
                self.base_y2_max = top2 if top2 > 0 else 1.0

                self.ax1.set_title(
                    f"{y1_label}{mode1_tag}  vs  {y2_label}{mode2_tag}",
                    fontsize=11, fontweight='bold', pad=4
                )

                lines1, labels1 = self.ax1.get_legend_handles_labels()
                lines2, labels2 = self.ax2.get_legend_handles_labels()
                all_lines = lines1 + lines2
                all_labels = labels1 + labels2
                ncol = 2 if len(all_labels) > 6 else 1
                self.ax1.legend(all_lines, all_labels, loc='upper left', fontsize=8, ncol=ncol, framealpha=0.82)

                stat_text += "   ||   " + self.format_stat(self.series2[0], len(self.series2))
            else:
                self.series2 = []
                self.ax1.set_title(f"{y1_label}{mode1_tag}", fontsize=11, fontweight='bold', pad=4)
                lines1, labels1 = self.ax1.get_legend_handles_labels()
                ncol = 2 if len(labels1) > 6 else 1
                self.ax1.legend(lines1, labels1, loc='upper left', fontsize=8, ncol=ncol, framealpha=0.82)

            self.ax1.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=16))
            self.ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            self.ax1.tick_params(axis='x', labelsize=9, rotation=0)
            self.ax1.grid(True, linestyle='--', alpha=0.6)

            self.vline = self.ax1.axvline(
                x=self.series1[0]['times'][0], color='gray', linestyle=':', linewidth=1.2, visible=False
            )

            self.lbl_stats.config(text="📊 (1번째 데이터 요약) " + stat_text)
            self.lbl_cursor.config(text="🎯 마우스 커서 위치 (각 축 1번째 데이터 기준): 그래프 위로 마우스를 올려주세요.")
            self.y1_entry.delete(0, tk.END)
            self.y2_entry.delete(0, tk.END)

            self.adjust_layout(draw=False)
            self.canvas.draw()

        except Exception as e:
            messagebox.showerror("오류", f"데이터를 처리하는 중 문제가 발생했습니다:\n{e}")

    # ------------------------------------------------------------------
    # 축 2 위치/배율 제어 및 자동 포개기
    # ------------------------------------------------------------------
    def update_ax2_transform(self, _=None):
        if not self.ax2 or not self.series2:
            return

        zoom = float(self.zoom_scale.get())
        offset_pct = float(self.offset_scale.get())

        span = self.base_y2_max / zoom
        shift = (offset_pct / 100.0) * span

        self.ax2.set_ylim(bottom=-shift, top=span - shift)
        self.canvas.draw_idle()

    def auto_align_ax2(self):
        if not self.ax2 or not self.series1 or not self.series2:
            messagebox.showinfo("안내", "축 1과 축 2에 데이터를 모두 불러온 상태에서 사용할 수 있습니다.")
            return

        ma1 = self.series1[0]['ma']
        ma2 = self.series2[0]['ma']

        y1_bot, y1_top = self.ax1.get_ylim()
        y1_span = y1_top - y1_bot

        avg1 = sum(ma1) / len(ma1)
        amp1 = max(max(ma1) - min(ma1), 1e-5)

        avg2 = sum(ma2) / len(ma2)
        amp2 = max(max(ma2) - min(ma2), 1e-5)

        target_y2_span = y1_span * (amp2 / amp1)
        pos_ratio = (avg1 - y1_bot) / y1_span
        new_bottom = avg2 - pos_ratio * target_y2_span
        new_top = new_bottom + target_y2_span

        self.ax2.set_ylim(bottom=new_bottom, top=new_top)
        self.adjust_layout(draw=False)
        self.canvas.draw()

    def on_mouse_scroll(self, event):
        if not self.ax2 or not event.inaxes:
            return

        step = 1 if event.button == 'up' else -1
        key = event.key or ""

        if 'shift' in key:
            cur_zoom = self.zoom_scale.get()
            new_zoom = max(0.2, min(10.0, cur_zoom + step * 0.2))
            self.zoom_scale.set(new_zoom)
            self.update_ax2_transform()
        elif 'control' in key or 'ctrl' in key:
            cur_offset = self.offset_scale.get()
            new_offset = max(-100, min(100, cur_offset + step * 5))
            self.offset_scale.set(new_offset)
            self.update_ax2_transform()

    def reset_view(self):
        self.offset_scale.set(0)
        self.zoom_scale.set(1.0)
        self.plot_data()

    # ------------------------------------------------------------------
    # 실시간 마우스 커서 추적 (각 축 첫 번째 데이터 Raw + MA 표시)
    # ------------------------------------------------------------------
    def find_nearest_index(self, times, target_dt):
        pos = bisect.bisect_left(times, target_dt)
        if pos == 0:
            return 0
        if pos == len(times):
            return len(times) - 1
        before = times[pos - 1]
        after = times[pos]
        return pos if (after - target_dt) < (target_dt - before) else pos - 1

    def on_mouse_move(self, event):
        if self._closing:
            return
        if not event.inaxes or not self.series1:
            if self.vline and self.vline.get_visible():
                self.vline.set_visible(False)
                self.canvas.draw_idle()
            return

        try:
            cursor_dt = mdates.num2date(event.xdata).replace(tzinfo=None)

            s1 = self.series1[0]
            idx1 = self.find_nearest_index(s1['times'], cursor_dt)
            t_str = s1['times'][idx1].strftime('%H:%M:%S')
            u1 = s1['unit']
            v1_str = (
                f"[축1] {s1['name']} "
                f"Raw: {s1['vals'][idx1]:,.2f}{u1} / MA({self.ma_window}): {s1['ma'][idx1]:,.2f}{u1}"
            )
            cursor_info = f"🎯 시각: {t_str}  |  {v1_str}"

            if self.series2:
                s2 = self.series2[0]
                idx2 = self.find_nearest_index(s2['times'], cursor_dt)
                u2 = s2['unit']
                v2_str = (
                    f"[축2] {s2['name']} "
                    f"Raw: {s2['vals'][idx2]:,.2f}{u2} / MA({self.ma_window}): {s2['ma'][idx2]:,.2f}{u2}"
                )
                cursor_info += f"  |  {v2_str}"

            self.lbl_cursor.config(text=cursor_info)

            if self.vline:
                self.vline.set_xdata([s1['times'][idx1], s1['times'][idx1]])
                self.vline.set_visible(True)
                self.canvas.draw_idle()
        except Exception:
            pass

    def apply_y_limit(self):
        y1_val = self.y1_entry.get().strip()
        y2_val = self.y2_entry.get().strip()
        try:
            if y1_val:
                self.ax1.set_ylim(bottom=0, top=float(y1_val))
            if y2_val and self.ax2:
                self.base_y2_max = float(y2_val)
                self.offset_scale.set(0)
                self.zoom_scale.set(1.0)
                self.ax2.set_ylim(bottom=0, top=self.base_y2_max)
            self.adjust_layout(draw=False)
            self.canvas.draw()
        except ValueError:
            messagebox.showwarning("입력 오류", "유효한 숫자를 입력해주세요 (예: 10000).")


if __name__ == "__main__":
    root = tk.Tk()
    app = DataVisualizerApp(root)
    root.mainloop()
