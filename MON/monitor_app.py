# python -m pip install matplotlib

# pip install pyinstaller
# pyinstaller --onefile --windowed monitor_app.py

import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.dates as mdates

class DataVisualizerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("이중 축 시스템 모니터링 시각화 도구")
        self.root.geometry("1000x750")

        self.file1_path = ""
        self.file2_path = ""
        self.ax2 = None  # 두 번째 축(오른쪽) 객체 저장용

        # 1. UI 컨트롤 프레임 구성
        control_frame = tk.Frame(self.root)
        control_frame.pack(pady=10)

        # 1행: 파일 선택 영역
        self.btn_file1 = tk.Button(control_frame, text="📁 데이터 1 (왼쪽 Y축)", command=self.load_file1, width=20)
        self.btn_file1.grid(row=0, column=0, padx=5, pady=5)
        self.lbl_file1 = tk.Label(control_frame, text="선택 안 됨", width=15, anchor="w")
        self.lbl_file1.grid(row=0, column=1, padx=5, pady=5)

        self.btn_file2 = tk.Button(control_frame, text="📁 데이터 2 (오른쪽 Y축)", command=self.load_file2, width=20)
        self.btn_file2.grid(row=0, column=2, padx=5, pady=5)
        self.lbl_file2 = tk.Label(control_frame, text="선택 안 됨 (선택 사항)", width=18, anchor="w")
        self.lbl_file2.grid(row=0, column=3, padx=5, pady=5)

        self.btn_plot = tk.Button(control_frame, text="📊 그래프 그리기", command=self.plot_data, bg="lightblue", font=("", 10, "bold"))
        self.btn_plot.grid(row=0, column=4, padx=10, pady=5)

        # 2행: Y축 제어 (Zoom) 영역
        zoom_frame = tk.Frame(self.root)
        zoom_frame.pack(pady=5)
        
        tk.Label(zoom_frame, text="🔍 왼쪽 Y축 최대:").grid(row=0, column=0, padx=5)
        self.y1_entry = tk.Entry(zoom_frame, width=10)
        self.y1_entry.grid(row=0, column=1, padx=5)

        tk.Label(zoom_frame, text="🔍 오른쪽 Y축 최대:").grid(row=0, column=2, padx=5)
        self.y2_entry = tk.Entry(zoom_frame, width=10)
        self.y2_entry.grid(row=0, column=3, padx=5)

        self.apply_btn = tk.Button(zoom_frame, text="적용 (Zoom)", command=self.apply_y_limit)
        self.apply_btn.grid(row=0, column=4, padx=10)
        
        # 엔터 키 바인딩
        self.y1_entry.bind('<Return>', lambda event: self.apply_y_limit())
        self.y2_entry.bind('<Return>', lambda event: self.apply_y_limit())

        # 2. Matplotlib 그래프 캔버스 초기화
        self.fig, self.ax1 = plt.subplots(figsize=(12, 6))
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.root)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def load_file1(self):
        path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if path:
            self.file1_path = path
            self.lbl_file1.config(text="..." + path[-15:])

    def load_file2(self):
        path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if path:
            self.file2_path = path
            self.lbl_file2.config(text="..." + path[-15:])

    def parse_file(self, file_path):
        times, values = [], []
        target_name = ""
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 3:
                    times.append(datetime.strptime(parts[0], '%Y%m%d%H%M%S'))
                    if not target_name: 
                        target_name = parts[1]
                    values.append(float(parts[2]))
        return times, values, target_name

    def get_moving_average(self, values, window_size=60):
        ma = []
        for i in range(len(values)):
            start_idx = max(0, i - window_size + 1)
            window_vals = values[start_idx:i+1]
            ma.append(sum(window_vals) / len(window_vals))
        return ma

    def plot_data(self):
        if not self.file1_path:
            messagebox.showwarning("경고", "데이터 1(왼쪽 축) 파일을 먼저 선택해주세요.")
            return

        # 그래프 완전 초기화
        self.ax1.clear()
        if self.ax2:
            self.ax2.remove()
            self.ax2 = None

        try:
            # --- 1. 왼쪽 Y축 데이터 그리기 ---
            times1, vals1, name1 = self.parse_file(self.file1_path)
            ma1 = self.get_moving_average(vals1, window_size=60)

            # 💡 [범례 수정] Raw Data와 60 이동평균(MA) 레이블 명시
            self.ax1.plot(times1, vals1, marker='', linestyle='-', color='dodgerblue', linewidth=0.5, alpha=0.6, label=f"{name1} Raw Data")
            self.ax1.plot(times1, ma1, marker='', linestyle='-', color='blue', linewidth=1.5, alpha=0.9, label=f"{name1} 60 MA Trend")
            
            self.ax1.set_xlabel("Time (HH:MM:SS)", fontsize=12)
            self.ax1.set_ylabel(name1, fontsize=12, color='blue', fontweight='bold')
            self.ax1.tick_params(axis='y', labelcolor='blue')
            self.ax1.set_ylim(bottom=0)
            
            # --- 2. 오른쪽 Y축 데이터 그리기 (선택 시) ---
            if self.file2_path:
                times2, vals2, name2 = self.parse_file(self.file2_path)
                ma2 = self.get_moving_average(vals2, window_size=60)

                self.ax2 = self.ax1.twinx()
                
                # 💡 [범례 수정] 두 번째 데이터도 Raw Data와 60 MA 레이블 명시
                self.ax2.plot(times2, vals2, marker='', linestyle='-', color='salmon', linewidth=0.5, alpha=0.6, label=f"{name2} Raw Data")
                self.ax2.plot(times2, ma2, marker='', linestyle='-', color='red', linewidth=1.5, alpha=0.9, label=f"{name2} 60 MA Trend")
                
                self.ax2.set_ylabel(name2, fontsize=12, color='red', fontweight='bold')
                self.ax2.tick_params(axis='y', labelcolor='red')
                self.ax2.set_ylim(bottom=0)
                
                self.ax1.set_title(f"{name1} vs {name2} Comparison", fontsize=16, fontweight='bold', pad=15)
                
                # 두 축의 범례 합치기
                lines1, labels1 = self.ax1.get_legend_handles_labels()
                lines2, labels2 = self.ax2.get_legend_handles_labels()
                self.ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
            else:
                self.ax1.set_title(name1, fontsize=16, fontweight='bold', pad=15)
                self.ax1.legend(loc='upper left')

            # 공통 속성 지정
            self.ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            self.fig.autofmt_xdate()
            self.ax1.grid(True, linestyle='--', alpha=0.6)
            
            self.y1_entry.delete(0, tk.END)
            self.y2_entry.delete(0, tk.END)
            self.canvas.draw()

        except Exception as e:
            messagebox.showerror("오류", f"데이터를 처리하는 중 문제가 발생했습니다:\n{e}")

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


