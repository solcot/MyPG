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
        self.root.title("DB 성능 로그 이중 축 분석기 Pro")
        self.root.geometry("1150x820")

        self.file1_path = ""
        self.file2_path = ""
        self.ax2 = None

        # 마우스 오버용 데이터 캐시
        self.times1, self.vals1, self.name1 = [], [], ""
        self.times2, self.vals2, self.name2 = [], [], ""
        self.vline = None

        # --- 1. 상단 컨트롤 영역 ---
        control_frame = tk.Frame(self.root)
        control_frame.pack(pady=8)

        # 파일 1 선택
        self.btn_file1 = tk.Button(control_frame, text="📁 데이터 1 (왼쪽 Y축)", command=self.load_file1, width=18)
        self.btn_file1.grid(row=0, column=0, padx=4, pady=4)
        self.lbl_file1 = tk.Label(control_frame, text="선택 안 됨", width=18, anchor="w", fg="gray")
        self.lbl_file1.grid(row=0, column=1, padx=4, pady=4)

        # 파일 2 선택
        self.btn_file2 = tk.Button(control_frame, text="📁 데이터 2 (오른쪽 Y축)", command=self.load_file2, width=18)
        self.btn_file2.grid(row=0, column=2, padx=4, pady=4)
        self.lbl_file2 = tk.Label(control_frame, text="선택 안 됨 (선택 사항)", width=18, anchor="w", fg="gray")
        self.lbl_file2.grid(row=0, column=3, padx=4, pady=4)

        # 파일 2 초기화 버튼
        self.btn_clear2 = tk.Button(control_frame, text="❌ 축2 해제", command=self.clear_file2)
        self.btn_clear2.grid(row=0, column=4, padx=4, pady=4)

        # 그래프 그리기 버튼
        self.btn_plot = tk.Button(control_frame, text="📊 그래프 그리기", command=self.plot_data, bg="lightblue", font=("", 10, "bold"), width=14)
        self.btn_plot.grid(row=0, column=5, padx=8, pady=4)

        # --- 2. 옵션 및 Y축 제어 (Zoom) 영역 ---
        opt_frame = tk.Frame(self.root)
        opt_frame.pack(pady=4)

        tk.Label(opt_frame, text="🔍 왼쪽 Y축 최대:").grid(row=0, column=0, padx=3)
        self.y1_entry = tk.Entry(opt_frame, width=9)
        self.y1_entry.grid(row=0, column=1, padx=3)

        tk.Label(opt_frame, text="🔍 오른쪽 Y축 최대:").grid(row=0, column=2, padx=3)
        self.y2_entry = tk.Entry(opt_frame, width=9)
        self.y2_entry.grid(row=0, column=3, padx=3)

        self.apply_btn = tk.Button(opt_frame, text="적용", command=self.apply_y_limit)
        self.apply_btn.grid(row=0, column=4, padx=5)

        self.reset_btn = tk.Button(opt_frame, text="원래대로", command=self.plot_data)
        self.reset_btn.grid(row=0, column=5, padx=5)

        # 이동평균(MA) 주기 설정
        tk.Label(opt_frame, text=" |  📈 MA 주기:").grid(row=0, column=6, padx=3)
        self.ma_entry = tk.Entry(opt_frame, width=5)
        self.ma_entry.insert(0, "60")
        self.ma_entry.grid(row=0, column=7, padx=3)

        # Raw Data 및 Peak 마커 표시 체크박스
        self.show_raw_var = tk.BooleanVar(value=True)
        tk.Checkbutton(opt_frame, text="Raw Data 표시", variable=self.show_raw_var, command=self.plot_data).grid(row=0, column=8, padx=6)

        self.show_peak_var = tk.BooleanVar(value=True)
        tk.Checkbutton(opt_frame, text="최고점(Peak) 표시", variable=self.show_peak_var, command=self.plot_data).grid(row=0, column=9, padx=6)

        # 엔터 키 바인딩
        self.y1_entry.bind('<Return>', lambda event: self.apply_y_limit())
        self.y2_entry.bind('<Return>', lambda event: self.apply_y_limit())
        self.ma_entry.bind('<Return>', lambda event: self.plot_data())

        # --- 3. 실시간 커서 정보 및 통계 요약 바 ---
        info_frame = tk.Frame(self.root, bg="#f0f4f8", bd=1, relief="solid")
        info_frame.pack(fill=tk.X, padx=15, pady=4)

        self.lbl_stats = tk.Label(info_frame, text="💡 파일을 선택하고 [그래프 그리기]를 눌러주세요.", bg="#f0f4f8", font=("Consolas", 10), anchor="w")
        self.lbl_stats.pack(fill=tk.X, padx=10, pady=3)

        self.lbl_cursor = tk.Label(info_frame, text="🎯 마우스 커서 위치: -", bg="#f0f4f8", fg="#333333", font=("Consolas", 10, "bold"), anchor="w")
        self.lbl_cursor.pack(fill=tk.X, padx=10, pady=2)

        # --- 4. Matplotlib 캔버스 및 기본 툴바 ---
        self.fig, self.ax1 = plt.subplots(figsize=(12, 5.8))
        self.fig.subplots_adjust(left=0.07, right=0.92, top=0.90, bottom=0.15)

        self.canvas = FigureCanvasTkAgg(self.fig, master=self.root)
        
        # 마우스 드래그 줌, 패닝, 이미지 저장이 가능한 기본 툴바 추가
        toolbar_frame = tk.Frame(self.root)
        toolbar_frame.pack(fill=tk.X, padx=15)
        self.toolbar = NavigationToolbar2Tk(self.canvas, toolbar_frame)
        self.toolbar.update()

        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # 마우스 이동 이벤트 연결 (십자선 및 값 추적)
        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)

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
                        continue  # 헤더나 잘못된 줄 자동 건너뛰기
        return times, values, target_name

    def get_moving_average(self, values, window_size=60):
        ma = []
        current_sum = 0.0
        # 슬라이딩 윈도우 방식으로 대용량 로그도 초고속 계산
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

        # 그래프 초기화
        self.ax1.clear()
        if self.ax2:
            self.ax2.remove()
            self.ax2 = None
        self.vline = None

        try:
            # --- 1. 왼쪽 Y축 데이터 ---
            self.times1, self.vals1, self.name1 = self.parse_file(self.file1_path)
            if not self.times1:
                raise ValueError("데이터 1 파일에서 유효한 데이터를 읽지 못했습니다.")
            ma1 = self.get_moving_average(self.vals1, window_size=ma_window)

            if self.show_raw_var.get():
                self.ax1.plot(self.times1, self.vals1, color='dodgerblue', linewidth=0.5, alpha=0.5, label=f"{self.name1} Raw Data")
            self.ax1.plot(self.times1, ma1, color='blue', linewidth=1.6, alpha=0.95, label=f"{self.name1} {ma_window} MA Trend")

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
                ma2 = self.get_moving_average(self.vals2, window_size=ma_window)

                self.ax2 = self.ax1.twinx()

                if self.show_raw_var.get():
                    self.ax2.plot(self.times2, self.vals2, color='salmon', linewidth=0.5, alpha=0.5, label=f"{self.name2} Raw Data")
                self.ax2.plot(self.times2, ma2, color='red', linewidth=1.6, alpha=0.95, label=f"{self.name2} {ma_window} MA Trend")

                if self.show_peak_var.get():
                    max_v2 = max(self.vals2)
                    max_t2 = self.times2[self.vals2.index(max_v2)]
                    self.ax2.scatter([max_t2], [max_v2], color='red', s=40, zorder=5, label=f"{self.name2} Peak ({max_v2:,.1f})")

                self.ax2.set_ylabel(self.name2, fontsize=12, color='red', fontweight='bold')
                self.ax2.tick_params(axis='y', labelcolor='red')
                self.ax2.set_ylim(bottom=0)

                self.ax1.set_title(f"{self.name1} vs {self.name2} Comparison", fontsize=15, fontweight='bold', pad=12)

                lines1, labels1 = self.ax1.get_legend_handles_labels()
                lines2, labels2 = self.ax2.get_legend_handles_labels()
                self.ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=9)

                stat_text += "   ||   " + self.format_stat(self.name2, self.times2, self.vals2)
            else:
                self.times2, self.vals2, self.name2 = [], [], ""
                self.ax1.set_title(self.name1, fontsize=15, fontweight='bold', pad=12)
                self.ax1.legend(loc='upper left', fontsize=9)

            # 공통 포맷 및 좌우 여백 제거
            self.ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            self.fig.autofmt_xdate()
            self.ax1.grid(True, linestyle='--', alpha=0.6)
            self.ax1.margins(x=0)  # 양 끝 빈칸 제거

            # 마우스 추적용 수직선 생성
            self.vline = self.ax1.axvline(x=self.times1[0], color='gray', linestyle=':', linewidth=1.2, visible=False)

            self.lbl_stats.config(text="📊 " + stat_text)
            self.y1_entry.delete(0, tk.END)
            self.y2_entry.delete(0, tk.END)
            self.canvas.draw()

        except Exception as e:
            messagebox.showerror("오류", f"데이터를 처리하는 중 문제가 발생했습니다:\n{e}")

    def on_mouse_move(self, event):
        if not event.inaxes or not self.times1:
            if self.vline and self.vline.get_visible():
                self.vline.set_visible(False)
                self.canvas.draw_idle()
            return

        try:
            # 마우스 X좌표를 시간으로 변환해 가장 가까운 인덱스 탐색
            cursor_dt = mdates.num2date(event.xdata).replace(tzinfo=None)
            idx1 = min(range(len(self.times1)), key=lambda i: abs(self.times1[i] - cursor_dt))
            t_str = self.times1[idx1].strftime('%H:%M:%S')
            v1_str = f"{self.name1}: {self.vals1[idx1]:,.2f}"

            cursor_info = f"🎯 시각: {t_str}  |  {v1_str}"

            if self.times2:
                idx2 = min(range(len(self.times2)), key=lambda i: abs(self.times2[i] - cursor_dt))
                cursor_info += f"  |  {self.name2}: {self.vals2[idx2]:,.2f}"

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
                self.ax2.set_ylim(bottom=0, top=float(y2_val))
            self.canvas.draw()
        except ValueError:
            messagebox.showwarning("입력 오류", "유효한 숫자를 입력해주세요 (예: 10000).")

if __name__ == "__main__":
    root = tk.Tk()
    app = DataVisualizerApp(root)
    root.mainloop()
