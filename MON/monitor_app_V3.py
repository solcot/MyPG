import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.dates as mdates

# 윈도우 한글 폰트 깨짐 방지 및 마이너스 기호 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

class DataVisualizerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DB 성능 로그 이중 축 분석기 Pro v3")
        self.root.geometry("1180x880")

        self.file1_path = ""
        self.file2_path = ""
        self.ax2 = None

        # 데이터 캐시 및 초기 Y축 범위 저장용
        self.times1, self.vals1, self.ma1, self.name1 = [], [], [], ""
        self.times2, self.vals2, self.ma2, self.name2 = [], [], [], ""
        self.base_y2_max = 100.0
        self.vline = None

        # --- 1. 상단 컨트롤 영역 ---
        control_frame = tk.Frame(self.root)
        control_frame.pack(pady=6)

        self.btn_file1 = tk.Button(control_frame, text="📁 데이터 1 (왼쪽 Y축)", command=self.load_file1, width=18)
        self.btn_file1.grid(row=0, column=0, padx=4, pady=3)
        self.lbl_file1 = tk.Label(control_frame, text="선택 안 됨", width=18, anchor="w", fg="gray")
        self.lbl_file1.grid(row=0, column=1, padx=4, pady=3)

        self.btn_file2 = tk.Button(control_frame, text="📁 데이터 2 (오른쪽 Y축)", command=self.load_file2, width=18)
        self.btn_file2.grid(row=0, column=2, padx=4, pady=3)
        self.lbl_file2 = tk.Label(control_frame, text="선택 안 됨 (선택 사항)", width=18, anchor="w", fg="gray")
        self.lbl_file2.grid(row=0, column=3, padx=4, pady=3)

        self.btn_clear2 = tk.Button(control_frame, text="❌ 축2 해제", command=self.clear_file2)
        self.btn_clear2.grid(row=0, column=4, padx=4, pady=3)

        self.btn_plot = tk.Button(control_frame, text="📊 그래프 그리기", command=self.plot_data, bg="lightblue", font=("", 10, "bold"), width=14)
        self.btn_plot.grid(row=0, column=5, padx=8, pady=3)

        # --- 2. 옵션 및 Y축 수동 제어 영역 ---
        opt_frame = tk.Frame(self.root)
        opt_frame.pack(pady=3)

        tk.Label(opt_frame, text="🔍 왼쪽 Y축 최대:").grid(row=0, column=0, padx=3)
        self.y1_entry = tk.Entry(opt_frame, width=8)
        self.y1_entry.grid(row=0, column=1, padx=3)

        tk.Label(opt_frame, text="🔍 오른쪽 Y축 최대:").grid(row=0, column=2, padx=3)
        self.y2_entry = tk.Entry(opt_frame, width=8)
        self.y2_entry.grid(row=0, column=3, padx=3)

        self.apply_btn = tk.Button(opt_frame, text="적용", command=self.apply_y_limit)
        self.apply_btn.grid(row=0, column=4, padx=4)

        self.reset_btn = tk.Button(opt_frame, text="원래대로", command=self.reset_view)
        self.reset_btn.grid(row=0, column=5, padx=4)

        tk.Label(opt_frame, text=" |  📈 MA 주기:").grid(row=0, column=6, padx=3)
        self.ma_entry = tk.Entry(opt_frame, width=5)
        self.ma_entry.insert(0, "60")
        self.ma_entry.grid(row=0, column=7, padx=3)

        self.show_raw_var = tk.BooleanVar(value=True)
        tk.Checkbutton(opt_frame, text="Raw Data 표시", variable=self.show_raw_var, command=self.plot_data).grid(row=0, column=8, padx=5)

        self.show_peak_var = tk.BooleanVar(value=True)
        tk.Checkbutton(opt_frame, text="최고점(Peak) 표시", variable=self.show_peak_var, command=self.plot_data).grid(row=0, column=9, padx=5)

        self.y1_entry.bind('<Return>', lambda event: self.apply_y_limit())
        self.y2_entry.bind('<Return>', lambda event: self.apply_y_limit())
        self.ma_entry.bind('<Return>', lambda event: self.plot_data())

        # --- 3. 💡 [신규] 데이터 2(빨간선) 단독 이동 및 스케일 조절 패널 ---
        move_frame = tk.Frame(self.root, bg="#fff5f5", bd=1, relief="groove")
        move_frame.pack(fill=tk.X, padx=15, pady=4)

        tk.Label(move_frame, text="🎛️ 데이터 2 위치/크기 제어:", bg="#fff5f5", fg="#c00000", font=("", 9, "bold")).pack(side=tk.LEFT, padx=8)

        # 상하 위치 이동 (Offset) 슬라이더 (-100% ~ +100%)
        tk.Label(move_frame, text="↕ 상하 이동(Offset):", bg="#fff5f5").pack(side=tk.LEFT, padx=4)
        self.offset_scale = tk.Scale(move_frame, from_=-100, to=100, orient=tk.HORIZONTAL, length=180, resolution=1,
                                     bg="#fff5f5", highlightthickness=0, command=self.update_ax2_transform)
        self.offset_scale.set(0)
        self.offset_scale.pack(side=tk.LEFT, padx=4)

        # 진폭 배율 확대/축소 (Scale) 슬라이더 (0.1배 ~ 10배)
        tk.Label(move_frame, text="  🔍 진폭 배율(Zoom):", bg="#fff5f5").pack(side=tk.LEFT, padx=4)
        self.zoom_scale = tk.Scale(move_frame, from_=0.2, to=10.0, orient=tk.HORIZONTAL, length=180, resolution=0.1,
                                   bg="#fff5f5", highlightthickness=0, command=self.update_ax2_transform)
        self.zoom_scale.set(1.0)
        self.zoom_scale.pack(side=tk.LEFT, padx=4)

        # 자동 포개기 버튼 & 위치 초기화 버튼
        self.btn_auto_align = tk.Button(move_frame, text="✨ 추세선 자동 포개기", command=self.auto_align_ax2, bg="#ffebeb", fg="#900000", font=("", 9, "bold"))
        self.btn_auto_align.pack(side=tk.LEFT, padx=10, pady=3)

        tk.Label(move_frame, text="(팁: Shift+마우스휠=진폭 조절 / Ctrl+마우스휠=상하 이동)", bg="#fff5f5", fg="#666666", font=("", 8)).pack(side=tk.RIGHT, padx=8)

        # --- 4. 실시간 커서 정보 및 통계 요약 바 ---
        info_frame = tk.Frame(self.root, bg="#f0f4f8", bd=1, relief="solid")
        info_frame.pack(fill=tk.X, padx=15, pady=3)

        self.lbl_stats = tk.Label(info_frame, text="💡 파일을 선택하고 [그래프 그리기]를 눌러주세요.", bg="#f0f4f8", font=("Consolas", 10), anchor="w")
        self.lbl_stats.pack(fill=tk.X, padx=10, pady=2)

        self.lbl_cursor = tk.Label(info_frame, text="🎯 마우스 커서 위치: -", bg="#f0f4f8", fg="#333333", font=("Consolas", 10, "bold"), anchor="w")
        self.lbl_cursor.pack(fill=tk.X, padx=10, pady=2)

        # --- 5. Matplotlib 캔버스 및 기본 툴바 ---
        self.fig, self.ax1 = plt.subplots(figsize=(12, 5.5))
        self.fig.subplots_adjust(left=0.07, right=0.92, top=0.90, bottom=0.15)

        self.canvas = FigureCanvasTkAgg(self.fig, master=self.root)

        toolbar_frame = tk.Frame(self.root)
        toolbar_frame.pack(fill=tk.X, padx=15)
        self.toolbar = NavigationToolbar2Tk(self.canvas, toolbar_frame)
        self.toolbar.update()

        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # 마우스 이벤트 연결 (커서 추적 및 마우스 휠 제어)
        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)
        self.canvas.mpl_connect('scroll_event', self.on_mouse_scroll)

    def load_file1(self):
        path = filedialog.askopenfilename(filetypes=[("Text/Log files", "*.txt *.log *.csv"), ("All files", "*.*")])
        if path:
            self.file1_path = path
            self.lbl_file1.config(text="..." + path[-20:], fg="blue")

    def load_file2(self):
        path = filedialog.askopenfilename(filetypes=[("Text/Log files", "*.txt *.log *.csv"), ("All files", "*.*")])
        if path:
            self.file2_path = path
            self.lbl_file2.config(text="..." + path[-20:], fg="red")

    def clear_file2(self):
        self.file2_path = ""
        self.lbl_file2.config(text="선택 안 됨 (선택 사항)", fg="gray")
        if self.file1_path:
            self.plot_data()

    def parse_file(self, file_path):
        times, values = [], []
        target_name = ""
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 3:
                    try:
                        times.append(datetime.strptime(parts[0], '%Y%m%d%H%M%S'))
                        if not target_name:
                            target_name = parts[1]
                        values.append(float(parts[2]))
                    except ValueError:
                        continue
        return times, values, target_name

    def get_moving_average(self, values, window_size=60):
        ma = []
        current_sum = 0.0
        for i in range(len(values)):
            current_sum += values[i]
            if i >= window_size:
                current_sum -= values[i - window_size]
                ma.append(current_sum / window_size)
            else:
                ma.append(current_sum / (i + 1))
        return ma

    def format_stat(self, name, times, vals):
        if not vals:
            return ""
        max_v = max(vals)
        min_v = min(vals)
        avg_v = sum(vals) / len(vals)
        max_t = times[vals.index(max_v)].strftime('%H:%M:%S')
        return f"[{name}] Max: {max_v:,.1f} ({max_t}) | Avg: {avg_v:,.1f} | Min: {min_v:,.1f}"

    def plot_data(self):
        if not self.file1_path:
            messagebox.showwarning("경고", "데이터 1(왼쪽 축) 파일을 먼저 선택해주세요.")
            return

        try:
            ma_window = max(1, int(self.ma_entry.get().strip()))
        except ValueError:
            ma_window = 60
            self.ma_entry.delete(0, tk.END)
            self.ma_entry.insert(0, "60")

        self.ax1.clear()
        if self.ax2:
            self.ax2.remove()
            self.ax2 = None
        self.vline = None

        # 슬라이더 초기화
        self.offset_scale.set(0)
        self.zoom_scale.set(1.0)

        try:
            # --- 1. 왼쪽 Y축 데이터 ---
            self.times1, self.vals1, self.name1 = self.parse_file(self.file1_path)
            if not self.times1:
                raise ValueError("데이터 1 파일에서 유효한 데이터를 읽지 못했습니다.")
            self.ma1 = self.get_moving_average(self.vals1, window_size=ma_window)

            if self.show_raw_var.get():
                self.ax1.plot(self.times1, self.vals1, color='dodgerblue', linewidth=0.5, alpha=0.45, label=f"{self.name1} Raw Data")
            self.ax1.plot(self.times1, self.ma1, color='blue', linewidth=1.6, alpha=0.95, label=f"{self.name1} {ma_window} MA Trend")

            if self.show_peak_var.get():
                max_v1 = max(self.vals1)
                max_t1 = self.times1[self.vals1.index(max_v1)]
                self.ax1.scatter([max_t1], [max_v1], color='blue', s=40, zorder=5, label=f"{self.name1} Peak ({max_v1:,.1f})")

            self.ax1.set_xlabel("Time (HH:MM:SS)", fontsize=11)
            self.ax1.set_ylabel(self.name1, fontsize=12, color='blue', fontweight='bold')
            self.ax1.tick_params(axis='y', labelcolor='blue')
            self.ax1.set_ylim(bottom=0)

            stat_text = self.format_stat(self.name1, self.times1, self.vals1)

            # --- 2. 오른쪽 Y축 데이터 (선택 시) ---
            if self.file2_path:
                self.times2, self.vals2, self.name2 = self.parse_file(self.file2_path)
                self.ma2 = self.get_moving_average(self.vals2, window_size=ma_window)

                self.ax2 = self.ax1.twinx()

                if self.show_raw_var.get():
                    self.ax2.plot(self.times2, self.vals2, color='salmon', linewidth=0.5, alpha=0.45, label=f"{self.name2} Raw Data")
                self.ax2.plot(self.times2, self.ma2, color='red', linewidth=1.6, alpha=0.95, label=f"{self.name2} {ma_window} MA Trend")

                if self.show_peak_var.get():
                    max_v2 = max(self.vals2)
                    max_t2 = self.times2[self.vals2.index(max_v2)]
                    self.ax2.scatter([max_t2], [max_v2], color='red', s=40, zorder=5, label=f"{self.name2} Peak ({max_v2:,.1f})")

                self.ax2.set_ylabel(self.name2, fontsize=12, color='red', fontweight='bold')
                self.ax2.tick_params(axis='y', labelcolor='red')
                self.ax2.set_ylim(bottom=0)

                # 초기 오른쪽 Y축 최대값 저장 (슬라이더 기준값)
                _, top2 = self.ax2.get_ylim()
                self.base_y2_max = top2 if top2 > 0 else 1.0

                self.ax1.set_title(f"{self.name1} vs {self.name2} Comparison", fontsize=15, fontweight='bold', pad=12)

                lines1, labels1 = self.ax1.get_legend_handles_labels()
                lines2, labels2 = self.ax2.get_legend_handles_labels()
                self.ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=9)

                stat_text += "   ||   " + self.format_stat(self.name2, self.times2, self.vals2)
            else:
                self.times2, self.vals2, self.ma2, self.name2 = [], [], [], ""
                self.ax1.set_title(self.name1, fontsize=15, fontweight='bold', pad=12)
                self.ax1.legend(loc='upper left', fontsize=9)

            self.ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            self.fig.autofmt_xdate()
            self.ax1.grid(True, linestyle='--', alpha=0.6)
            self.ax1.margins(x=0)

            self.vline = self.ax1.axvline(x=self.times1[0], color='gray', linestyle=':', linewidth=1.2, visible=False)

            self.lbl_stats.config(text="📊 " + stat_text)
            self.y1_entry.delete(0, tk.END)
            self.y2_entry.delete(0, tk.END)
            self.canvas.draw()

        except Exception as e:
            messagebox.showerror("오류", f"데이터를 처리하는 중 문제가 발생했습니다:\n{e}")

    def update_ax2_transform(self, _=None):
        """슬라이더 조작 시 데이터 2(오른쪽 축)의 상하 위치와 진폭 배율을 즉시 반영"""
        if not self.ax2 or not self.vals2:
            return

        zoom = float(self.zoom_scale.get())
        offset_pct = float(self.offset_scale.get())

        # 배율(zoom)이 커질수록 Y축 표시 범위를 좁혀서 그래프 진폭을 키움
        span = self.base_y2_max / zoom
        # 상하 이동(offset_pct)이 양수(+)면 그래프가 위로 올라가도록 하단 시작점을 낮춤
        shift = (offset_pct / 100.0) * span

        new_bottom = -shift
        new_top = span - shift

        self.ax2.set_ylim(bottom=new_bottom, top=new_top)
        self.canvas.draw_idle()

    def auto_align_ax2(self):
        """데이터 2의 이동평균(MA) 추세선을 데이터 1의 추세선 위치와 진폭에 자동으로 포개기"""
        if not self.ax2 or not self.ma1 or not self.ma2:
            messagebox.showinfo("안내", "데이터 1과 데이터 2를 모두 불러온 상태에서 사용할 수 있습니다.")
            return

        # 데이터 1의 현재 화면상 높이 비율 계산
        y1_bot, y1_top = self.ax1.get_ylim()
        y1_span = y1_top - y1_bot

        avg1 = sum(self.ma1) / len(self.ma1)
        min1, max1 = min(self.ma1), max(self.ma1)
        amp1 = max(max1 - min1, 1e-5)

        avg2 = sum(self.ma2) / len(self.ma2)
        min2, max2 = min(self.ma2), max(self.ma2)
        amp2 = max(max2 - min2, 1e-5)

        # 두 추세선의 진폭 비율을 맞춰 오른쪽 Y축 전체 폭(Span) 계산
        target_y2_span = y1_span * (amp2 / amp1)
        # 데이터 1 평균이 위치한 화면 높이 비율(ratio)에 데이터 2 평균이 오도록 설정
        pos_ratio = (avg1 - y1_bot) / y1_span
        new_bottom = avg2 - pos_ratio * target_y2_span
        new_top = new_bottom + target_y2_span

        self.ax2.set_ylim(bottom=new_bottom, top=new_top)
        self.canvas.draw()

    def on_mouse_scroll(self, event):
        """Shift + 휠: 데이터 2 진폭 확대/축소 | Ctrl + 휠: 데이터 2 상하 이동"""
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
        """슬라이더 및 Y축 범위를 초기 상태로 복원"""
        self.offset_scale.set(0)
        self.zoom_scale.set(1.0)
        self.plot_data()

    def on_mouse_move(self, event):
        if not event.inaxes or not self.times1:
            if self.vline and self.vline.get_visible():
                self.vline.set_visible(False)
                self.canvas.draw_idle()
            return

        try:
            cursor_dt = mdates.num2date(event.xdata).replace(tzinfo=None)
            idx1 = min(range(len(self.times1)), key=lambda i: abs(self.times1[i] - cursor_dt))
            t_str = self.times1[idx1].strftime('%H:%M:%S')
            v1_str = f"{self.name1}: {self.vals1[idx1]:,.2f} (MA: {self.ma1[idx1]:,.1f})"

            cursor_info = f"🎯 시각: {t_str}  |  {v1_str}"

            if self.times2:
                idx2 = min(range(len(self.times2)), key=lambda i: abs(self.times2[i] - cursor_dt))
                cursor_info += f"  |  {self.name2}: {self.vals2[idx2]:,.2f} (MA: {self.ma2[idx2]:,.1f})"

            self.lbl_cursor.config(text=cursor_info)

            if self.vline:
                self.vline.set_xdata([self.times1[idx1], self.times1[idx1]])
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
            self.canvas.draw()
        except ValueError:
            messagebox.showwarning("입력 오류", "유효한 숫자를 입력해주세요 (예: 10000).")

if __name__ == "__main__":
    root = tk.Tk()
    app = DataVisualizerApp(root)
    root.mainloop()
