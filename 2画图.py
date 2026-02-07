import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams
import os
from math import floor, log10

# 设置中文字体
rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
rcParams['axes.unicode_minus'] = False

def format_significant_figures(value, sig_figs=3):
    """格式化数字为指定的有效数字"""
    if value == 0:
        return "0"
    
    # 计算数量级
    magnitude = floor(log10(abs(value)))
    
    # 如果是整数且位数较多,使用千分位分隔符
    if magnitude >= sig_figs - 1:
        # 对于大数字,保留有效数字并添加千分位分隔符
        decimal_places = max(0, sig_figs - magnitude - 1)
        formatted = f"{value:,.{decimal_places}f}"
    else:
        # 对于小数,保留有效数字
        decimal_places = sig_figs - magnitude - 1
        formatted = f"{value:.{decimal_places}f}"
    
    return formatted

def get_excel_file():
    """交互式获取Excel文件名"""
    print("=" * 60)
    print("欢迎使用期货数据可视化分析程序")
    print("=" * 60)
    
    while True:
        excel_file = input("\n请输入Excel文件名(包含扩展名): ").strip()
        
        if not excel_file:
            print("✗ 文件名不能为空,请重新输入")
            continue
        
        if not os.path.exists(excel_file):
            print(f"✗ 错误: 找不到文件 '{excel_file}'")
            retry = input("是否重新输入文件名?(y/n): ").strip().lower()
            if retry != 'y':
                return None
            continue
        
        return excel_file


def validate_columns(df):
    """验证Excel文件是否包含所需的列"""
    required_columns = [
        '时间', '持仓量', '持仓价值(USD)',
        '大户账户多空比', '大户多头账户占比', '大户空头账户占比',
        '大户持仓多空比', '大户多头持仓占比', '大户空头持仓占比',
        '全市场多空比', '全市场多头人数占比', '全市场空头人数占比',
        '基差', '基差率', '资金费率'
    ]
    
    # 可选列
    optional_columns = ['收盘价', '开盘价', '最高价', '最低价']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print("\n✗ 错误: Excel文件中缺少以下必需列:")
        for col in missing_columns:
            print(f"   - {col}")
        return False
    
    # 检查可选列并提示
    missing_optional = [col for col in optional_columns if col not in df.columns]
    if missing_optional:
        print("\n⚠️  提示: Excel文件中缺少以下可选列:")
        for col in missing_optional:
            print(f"   - {col}")
        print("   (这些列缺失不影响程序运行,相关图表将不显示对应数据)")
    
    return True


def create_figure(df, plot_func, title):
    """创建单个图表"""
    fig = plt.figure(figsize=(14, 8))
    ax = fig.add_subplot(111)
    plot_func(ax, df)
    fig.suptitle(title, fontsize=16, fontweight='bold')
    plt.tight_layout()
    return fig


def plot1_position(ax, df):
    """图1: 持仓量和收盘价(双轴图表)"""
    # 检查是否有收盘价数据
    has_close_price = '收盘价' in df.columns
    
    if not has_close_price:
        print("⚠️  警告: 缺少'收盘价'列,无法绘制价格走势")
        # 只画持仓量
        line1, = ax.plot(df['时间'], df['持仓量'], 'b-', label='持仓量', linewidth=2, picker=5)
        
        ax.set_xlabel('时间', fontsize=12)
        ax.set_ylabel('持仓量', color='b', fontsize=12)
        ax.tick_params(axis='y', labelcolor='b')
        
        lines = [line1]
        axes_list = [ax]
    else:
        # 有收盘价数据 - 双轴图表
        ax_twin = ax.twinx()
        
        line1, = ax.plot(df['时间'], df['持仓量'], 'b-', label='持仓量', linewidth=2, picker=5)
        line2, = ax_twin.plot(df['时间'], df['收盘价'], 'orange', label='收盘价', linewidth=2.5, alpha=0.8, picker=5)
        
        ax.set_xlabel('时间', fontsize=12)
        ax.set_ylabel('持仓量', color='b', fontsize=12)
        ax_twin.set_ylabel('收盘价', color='orange', fontsize=12)
        
        ax.tick_params(axis='y', labelcolor='b')
        ax_twin.tick_params(axis='y', labelcolor='orange')
        
        lines = [line1, line2]
        axes_list = [ax, ax_twin]
    
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=10)
    
    # 在图片右下角添加固定文本框
    annot = ax.text(0.98, 0.02, "", transform=ax.transAxes, 
                   fontsize=10, verticalalignment='bottom', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.8))
    annot.set_visible(False)
    
    def on_hover(event):
        if event.inaxes in axes_list:
            vis = annot.get_visible()
            # 检测所有线
            for i, line in enumerate(lines):
                cont, ind = line.contains(event)
                if cont:
                    idx = ind["ind"][0]
                    y_val = line.get_ydata()[idx]
                    label = line.get_label()
                    
                    # 从DataFrame中获取原始时间
                    time_str = df.iloc[idx]['时间'].strftime('%Y-%m-%d %H:%M')
                    
                    # 使用3位有效数字格式化
                    formatted_val = format_significant_figures(y_val, 3)
                    text = f"时间: {time_str}\n{label}: {formatted_val}"
                    
                    annot.set_text(text)
                    annot.set_visible(True)
                    ax.figure.canvas.draw_idle()
                    return
            if vis:
                annot.set_visible(False)
                ax.figure.canvas.draw_idle()
    
    ax.figure.canvas.mpl_connect("motion_notify_event", on_hover)



def plot2_account_ratio(ax, df):
    """图2: 大户账户多空比 + 价格"""
    # 检查是否有收盘价数据
    has_close_price = '收盘价' in df.columns
    
    if has_close_price:
        # 创建双轴
        ax_twin = ax.twinx()
        
        line1, = ax.plot(df['时间'], df['大户账户多空比'], 'purple', label='大户账户多空比', linewidth=2.5, picker=5)
        line2, = ax_twin.plot(df['时间'], df['收盘价'], 'orange', label='收盘价', linewidth=2.5, alpha=0.8, picker=5)
        
        ax.set_ylabel('多空比', color='purple', fontsize=12)
        ax_twin.set_ylabel('收盘价', color='orange', fontsize=12)
        ax_twin.tick_params(axis='y', labelcolor='orange')
        
        lines = [line1, line2]
        axes_list = [ax, ax_twin]
    else:
        line1, = ax.plot(df['时间'], df['大户账户多空比'], 'purple', label='大户账户多空比', linewidth=2.5, picker=5)
        
        ax.set_ylabel('多空比', color='purple', fontsize=12)
        
        lines = [line1]
        axes_list = [ax]
    
    ax.set_xlabel('时间', fontsize=12)
    ax.tick_params(axis='y', labelcolor='purple')
    ax.grid(True, alpha=0.3)
    ax.axhline(y=1, color='gray', linestyle='--', alpha=0.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=10)
    
    # 在图片右下角添加固定文本框
    annot = ax.text(0.98, 0.02, "", transform=ax.transAxes, 
                   fontsize=10, verticalalignment='bottom', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.8))
    annot.set_visible(False)
    
    def on_hover(event):
        if event.inaxes in axes_list:
            vis = annot.get_visible()
            for line in lines:
                cont, ind = line.contains(event)
                if cont:
                    idx = ind["ind"][0]
                    y_val = line.get_ydata()[idx]
                    label = line.get_label()
                    
                    time_str = df.iloc[idx]['时间'].strftime('%Y-%m-%d %H:%M')
                    formatted_val = format_significant_figures(y_val, 3)
                    text = f"时间: {time_str}\n{label}: {formatted_val}"
                    annot.set_text(text)
                    annot.set_visible(True)
                    ax.figure.canvas.draw_idle()
                    return
            if vis:
                annot.set_visible(False)
                ax.figure.canvas.draw_idle()
    
    ax.figure.canvas.mpl_connect("motion_notify_event", on_hover)

def plot3_position_ratio(ax, df):
    """图3: 大户持仓多空比 + 价格"""
    has_close_price = '收盘价' in df.columns
    
    if has_close_price:
        ax_twin = ax.twinx()
        
        line1, = ax.plot(df['时间'], df['大户持仓多空比'], 'purple', label='大户持仓多空比', linewidth=2.5, picker=5)
        line2, = ax_twin.plot(df['时间'], df['收盘价'], 'orange', label='收盘价', linewidth=2, alpha=0.7, picker=5)
        
        ax.set_ylabel('多空比', color='purple', fontsize=12)
        ax_twin.set_ylabel('收盘价', color='orange', fontsize=12)
        ax_twin.tick_params(axis='y', labelcolor='orange')
        
        lines = [line1, line2]
        axes_list = [ax, ax_twin]
    else:
        line1, = ax.plot(df['时间'], df['大户持仓多空比'], 'purple', label='大户持仓多空比', linewidth=2.5, picker=5)
        
        ax.set_ylabel('多空比', color='purple', fontsize=12)
        
        lines = [line1]
        axes_list = [ax]
    
    ax.set_xlabel('时间', fontsize=12)
    ax.tick_params(axis='y', labelcolor='purple')
    ax.grid(True, alpha=0.3)
    ax.axhline(y=1, color='gray', linestyle='--', alpha=0.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=10)
    
    annot = ax.text(0.98, 0.02, "", transform=ax.transAxes, 
                   fontsize=10, verticalalignment='bottom', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.8))
    annot.set_visible(False)
    
    def on_hover(event):
        if event.inaxes in axes_list:
            vis = annot.get_visible()
            for line in lines:
                cont, ind = line.contains(event)
                if cont:
                    idx = ind["ind"][0]
                    y_val = line.get_ydata()[idx]
                    label = line.get_label()
                    
                    time_str = df.iloc[idx]['时间'].strftime('%Y-%m-%d %H:%M')
                    formatted_val = format_significant_figures(y_val, 3)
                    text = f"时间: {time_str}\n{label}: {formatted_val}"
                    annot.set_text(text)
                    annot.set_visible(True)
                    ax.figure.canvas.draw_idle()
                    return
            if vis:
                annot.set_visible(False)
                ax.figure.canvas.draw_idle()
    
    ax.figure.canvas.mpl_connect("motion_notify_event", on_hover)

def plot4_market_ratio(ax, df):
    """图4: 全市场多空比 + 价格"""
    has_close_price = '收盘价' in df.columns
    
    if has_close_price:
        ax_twin = ax.twinx()
        
        line1, = ax.plot(df['时间'], df['全市场多空比'], 'purple', label='全市场多空比', linewidth=2.5, picker=5)
        line2, = ax_twin.plot(df['时间'], df['收盘价'], 'orange', label='收盘价', linewidth=2, alpha=0.7, picker=5)
        
        ax.set_ylabel('多空比', color='purple', fontsize=12)
        ax_twin.set_ylabel('收盘价', color='orange', fontsize=12)
        ax_twin.tick_params(axis='y', labelcolor='orange')
        
        lines = [line1, line2]
        axes_list = [ax, ax_twin]
    else:
        line1, = ax.plot(df['时间'], df['全市场多空比'], 'purple', label='全市场多空比', linewidth=2.5, picker=5)
        
        ax.set_ylabel('多空比', color='purple', fontsize=12)
        
        lines = [line1]
        axes_list = [ax]
    
    ax.set_xlabel('时间', fontsize=12)
    ax.tick_params(axis='y', labelcolor='purple')
    ax.grid(True, alpha=0.3)
    ax.axhline(y=1, color='gray', linestyle='--', alpha=0.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=10)
    
    annot = ax.text(0.98, 0.02, "", transform=ax.transAxes, 
                   fontsize=10, verticalalignment='bottom', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.8))
    annot.set_visible(False)
    
    def on_hover(event):
        if event.inaxes in axes_list:
            vis = annot.get_visible()
            for line in lines:
                cont, ind = line.contains(event)
                if cont:
                    idx = ind["ind"][0]
                    y_val = line.get_ydata()[idx]
                    label = line.get_label()
                    
                    time_str = df.iloc[idx]['时间'].strftime('%Y-%m-%d %H:%M')
                    formatted_val = format_significant_figures(y_val, 3)
                    text = f"时间: {time_str}\n{label}: {formatted_val}"
                    annot.set_text(text)
                    annot.set_visible(True)
                    ax.figure.canvas.draw_idle()
                    return
            if vis:
                annot.set_visible(False)
                ax.figure.canvas.draw_idle()
    
    ax.figure.canvas.mpl_connect("motion_notify_event", on_hover)

def plot5_basis(ax, df):
    """图5: 基差和基差率 + 价格"""
    has_close_price = '收盘价' in df.columns
    
    if has_close_price:
        ax_twin = ax.twinx()
        ax_twin2 = ax.twinx()
        ax_twin2.spines['right'].set_position(('outward', 60))
        
        line1, = ax.plot(df['时间'], df['基差'], 'b-', label='基差', linewidth=2, picker=5)
        line2, = ax_twin.plot(df['时间'], df['基差率'], 'r-', label='基差率(%)', linewidth=2, picker=5)
        line3, = ax_twin2.plot(df['时间'], df['收盘价'], 'orange', label='收盘价', linewidth=2.5, alpha=0.8, picker=5)
        
        ax.set_ylabel('基差', color='b', fontsize=12)
        ax_twin.set_ylabel('基差率(%)', color='r', fontsize=12)
        ax_twin2.set_ylabel('收盘价', color='orange', fontsize=12)
        ax_twin.tick_params(axis='y', labelcolor='r')
        ax_twin2.tick_params(axis='y', labelcolor='orange')
        
        lines = [line1, line2, line3]
        axes_list = [ax, ax_twin, ax_twin2]
    else:
        ax_twin = ax.twinx()
        
        line1, = ax.plot(df['时间'], df['基差'], 'b-', label='基差', linewidth=2, picker=5)
        line2, = ax_twin.plot(df['时间'], df['基差率'], 'r-', label='基差率(%)', linewidth=2, picker=5)
        
        ax.set_ylabel('基差', color='b', fontsize=12)
        ax_twin.set_ylabel('基差率(%)', color='r', fontsize=12)
        ax_twin.tick_params(axis='y', labelcolor='r')
        
        lines = [line1, line2]
        axes_list = [ax, ax_twin]
    
    ax.set_xlabel('时间', fontsize=12)
    ax.tick_params(axis='y', labelcolor='b')
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=10)
    
    annot = ax.text(0.98, 0.02, "", transform=ax.transAxes, 
                   fontsize=10, verticalalignment='bottom', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.8))
    annot.set_visible(False)
    
    def on_hover(event):
        if event.inaxes in axes_list:
            vis = annot.get_visible()
            for line in lines:
                cont, ind = line.contains(event)
                if cont:
                    idx = ind["ind"][0]
                    y_val = line.get_ydata()[idx]
                    label = line.get_label()
                    
                    time_str = df.iloc[idx]['时间'].strftime('%Y-%m-%d %H:%M')
                    formatted_val = format_significant_figures(y_val, 3)
                    text = f"时间: {time_str}\n{label}: {formatted_val}"
                    annot.set_text(text)
                    annot.set_visible(True)
                    ax.figure.canvas.draw_idle()
                    return
            if vis:
                annot.set_visible(False)
                ax.figure.canvas.draw_idle()
    
    ax.figure.canvas.mpl_connect("motion_notify_event", on_hover)


def plot6_funding_rate(ax, df):
    """图6: 资金费率 + 收盘价"""
    has_close_price = '收盘价' in df.columns
    has_funding_rate = '资金费率' in df.columns
    
    if not has_funding_rate:
        print("⚠️  警告: 缺少'资金费率'列,无法绘制资金费率图表")
        ax.text(0.5, 0.5, '缺少资金费率数据', 
               transform=ax.transAxes, 
               fontsize=16, 
               ha='center', 
               va='center')
        return
    
    # 将资金费率转换为百分比(乘以100)
    funding_rate_percent = df['资金费率'] * 100
    
    if has_close_price:
        ax_twin = ax.twinx()
        
        # 资金费率:使用线条+明显的标记点
        line1, = ax.plot(df['时间'], funding_rate_percent, 'b-o', 
                        label='资金费率(%)', linewidth=2, 
                        markersize=6, markerfacecolor='blue', 
                        markeredgecolor='darkblue', markeredgewidth=1,
                        picker=5)
        
        # 收盘价:只用线条
        line2, = ax_twin.plot(df['时间'], df['收盘价'], 'orange', 
                             label='收盘价', linewidth=2.5, alpha=0.8, picker=5)
        
        ax.set_ylabel('资金费率(%)', color='b', fontsize=12)
        ax_twin.set_ylabel('收盘价', color='orange', fontsize=12)
        ax.tick_params(axis='y', labelcolor='b')
        ax_twin.tick_params(axis='y', labelcolor='orange')
        
        lines = [line1, line2]
        axes_list = [ax, ax_twin]
    else:
        line1, = ax.plot(df['时间'], funding_rate_percent, 'b-o', 
                        label='资金费率(%)', linewidth=2, 
                        markersize=6, markerfacecolor='blue', 
                        markeredgecolor='darkblue', markeredgewidth=1,
                        picker=5)
        ax.set_ylabel('资金费率(%)', color='b', fontsize=12)
        ax.tick_params(axis='y', labelcolor='b')
        lines = [line1]
        axes_list = [ax]
    
    ax.set_xlabel('时间', fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=10)
    
    # 在图片右下角添加固定文本框
    annot = ax.text(0.98, 0.02, "", transform=ax.transAxes, 
                   fontsize=10, verticalalignment='bottom', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.8))
    annot.set_visible(False)
    
    def on_hover(event):
        if event.inaxes in axes_list:
            vis = annot.get_visible()
            for line in lines:
                cont, ind = line.contains(event)
                if cont:
                    idx = ind["ind"][0]
                    y_val = line.get_ydata()[idx]
                    label = line.get_label()
                    
                    time_str = df.iloc[idx]['时间'].strftime('%Y-%m-%d %H:%M')
                    formatted_val = format_significant_figures(y_val, 3)
                    text = f"时间: {time_str}\n{label}: {formatted_val}"
                    
                    annot.set_text(text)
                    annot.set_visible(True)
                    ax.figure.canvas.draw_idle()
                    return
            if vis:
                annot.set_visible(False)
                ax.figure.canvas.draw_idle()
    
    ax.figure.canvas.mpl_connect("motion_notify_event", on_hover)


def plot7_position_value_price(ax, df):
    """图7: 持仓市值比与收盘价"""
    has_close_price = '收盘价' in df.columns
    
    if not has_close_price:
        print("⚠️  警告: 缺少'收盘价'列,无法绘制价格走势")
        # 只画持仓市值比
        line1, = ax.plot(df['时间'], df['持仓市值比'], 'b-', label='持仓市值比', linewidth=2.5, picker=5)
        
        ax.set_xlabel('时间', fontsize=12)
        ax.set_ylabel('持仓市值比', color='b', fontsize=12)
        ax.tick_params(axis='y', labelcolor='b')
        
        lines = [line1]
        axes_list = [ax]
    else:
        # 创建双轴图表
        ax_twin = ax.twinx()
        
        line1, = ax.plot(df['时间'], df['持仓市值比'], 'b-', label='持仓市值比', linewidth=2.5, picker=5)
        line2, = ax_twin.plot(df['时间'], df['收盘价'], 'orange', label='收盘价', linewidth=2.5, alpha=0.8, picker=5)
        
        ax.set_xlabel('时间', fontsize=12)
        ax.set_ylabel('持仓市值比', color='b', fontsize=12)
        ax_twin.set_ylabel('收盘价', color='orange', fontsize=12)
        
        ax.tick_params(axis='y', labelcolor='b')
        ax_twin.tick_params(axis='y', labelcolor='orange')
        
        lines = [line1, line2]
        axes_list = [ax, ax_twin]
    
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc='upper left', fontsize=10)
    
    # 在图片右下角添加固定文本框
    annot = ax.text(0.98, 0.02, "", transform=ax.transAxes, 
                   fontsize=10, verticalalignment='bottom', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.8))
    annot.set_visible(False)
    
    def on_hover(event):
        if event.inaxes in axes_list:
            vis = annot.get_visible()
            for line in lines:
                cont, ind = line.contains(event)
                if cont:
                    idx = ind["ind"][0]
                    y_val = line.get_ydata()[idx]
                    label = line.get_label()
                    
                    time_str = df.iloc[idx]['时间'].strftime('%Y-%m-%d %H:%M')
                    formatted_val = format_significant_figures(y_val, 3)
                    text = f"时间: {time_str}\n{label}: {formatted_val}"
                    
                    annot.set_text(text)
                    annot.set_visible(True)
                    ax.figure.canvas.draw_idle()
                    return
            if vis:
                annot.set_visible(False)
                ax.figure.canvas.draw_idle()
    
    ax.figure.canvas.mpl_connect("motion_notify_event", on_hover)


def calculate_period_funding_rate(df):
    """计算特定时间段的平均资金费率"""
    print("\n" + "=" * 60)
    print("计算时间段平均资金费率")
    print("=" * 60)
    
    # 检查是否有资金费率数据
    if '资金费率' not in df.columns:
        print("⚠️  警告: 缺少'资金费率'列,无法计算")
        return
    
    # 显示数据的时间范围
    min_date = df['时间'].min()
    max_date = df['时间'].max()
    print(f"\n数据时间范围: {min_date.strftime('%Y-%m-%d %H:%M')} 至 {max_date.strftime('%Y-%m-%d %H:%M')}")
    
    while True:
        try:
            print("\n请输入时间段(格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM)")
            start_str = input("起始时间: ").strip()
            end_str = input("结束时间: ").strip()
            
            if not start_str or not end_str:
                print("✗ 时间不能为空")
                continue
            
            # 尝试解析时间
            try:
                start_time = pd.to_datetime(start_str)
                end_time = pd.to_datetime(end_str)
            except:
                print("✗ 时间格式错误,请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM 格式")
                continue
            
            if start_time >= end_time:
                print("✗ 起始时间必须早于结束时间")
                continue
            
            # 筛选时间段内的数据
            mask = (df['时间'] >= start_time) & (df['时间'] <= end_time)
            period_df = df[mask]
            
            if len(period_df) == 0:
                print(f"✗ 在指定时间段内没有找到数据")
                continue
            
            # 过滤掉资金费率为0或NaN的数据点(这些可能是无效数据)
            valid_funding_rate = period_df['资金费率'].dropna()
            valid_funding_rate = valid_funding_rate[valid_funding_rate != 0]
            
            if len(valid_funding_rate) == 0:
                print(f"✗ 在指定时间段内没有有效的资金费率数据")
                continue
            
            # 计算平均值(转换为百分比)
            avg_funding_rate = valid_funding_rate.mean() * 100
            
            # 计算最大值和最小值
            max_funding_rate = valid_funding_rate.max() * 100
            min_funding_rate = valid_funding_rate.min() * 100
            
            # 显示结果
            print("\n" + "-" * 60)
            print(f"时间段: {start_time.strftime('%Y-%m-%d %H:%M')} 至 {end_time.strftime('%Y-%m-%d %H:%M')}")
            print(f"有效数据点数: {len(valid_funding_rate)} (总数据点: {len(period_df)})")
            print(f"平均资金费率: {format_significant_figures(avg_funding_rate, 4)}%")
            print(f"最大资金费率: {format_significant_figures(max_funding_rate, 4)}%")
            print(f"最小资金费率: {format_significant_figures(min_funding_rate, 4)}%")
            print("-" * 60)
            
            break
            
        except Exception as e:
            print(f"✗ 发生错误: {e}")
            retry = input("是否重新输入?(y/n): ").strip().lower()
            if retry != 'y':
                break


def calculate_period_average(df):
    """计算特定时间段的平均基差和基差率"""
    print("\n" + "=" * 60)
    print("计算时间段平均值")
    print("=" * 60)
    
    # 显示数据的时间范围
    min_date = df['时间'].min()
    max_date = df['时间'].max()
    print(f"\n数据时间范围: {min_date.strftime('%Y-%m-%d %H:%M')} 至 {max_date.strftime('%Y-%m-%d %H:%M')}")
    
    while True:
        try:
            print("\n请输入时间段(格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM)")
            start_str = input("起始时间: ").strip()
            end_str = input("结束时间: ").strip()
            
            if not start_str or not end_str:
                print("✗ 时间不能为空")
                continue
            
            # 尝试解析时间
            try:
                start_time = pd.to_datetime(start_str)
                end_time = pd.to_datetime(end_str)
            except:
                print("✗ 时间格式错误,请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM 格式")
                continue
            
            if start_time >= end_time:
                print("✗ 起始时间必须早于结束时间")
                continue
            
            # 筛选时间段内的数据
            mask = (df['时间'] >= start_time) & (df['时间'] <= end_time)
            period_df = df[mask]
            
            if len(period_df) == 0:
                print(f"✗ 在指定时间段内没有找到数据")
                continue
            
            # 计算平均值
            avg_basis = period_df['基差'].mean()
            avg_basis_rate = period_df['基差率'].mean()
            
            # 显示结果
            print("\n" + "-" * 60)
            print(f"时间段: {start_time.strftime('%Y-%m-%d %H:%M')} 至 {end_time.strftime('%Y-%m-%d %H:%M')}")
            print(f"数据点数: {len(period_df)}")
            print(f"平均基差: {format_significant_figures(avg_basis, 4)}")
            print(f"平均基差率: {format_significant_figures(avg_basis_rate, 4)}%")
            print("-" * 60)
            
            break
            
        except Exception as e:
            print(f"✗ 发生错误: {e}")
            retry = input("是否重新输入?(y/n): ").strip().lower()
            if retry != 'y':
                break


def plot_futures_analysis(excel_file):
    """生成7张独立的期货分析图表"""
    df = pd.read_excel(excel_file)
    df['时间'] = pd.to_datetime(df['时间'])
    
    # 创建7个独立的图表窗口
    create_figure(df, plot1_position, '图1: 持仓量和价格')
    create_figure(df, plot2_account_ratio, '图2: 大户账户多空比 + 价格')
    create_figure(df, plot3_position_ratio, '图3: 大户持仓多空比 + 价格')
    create_figure(df, plot4_market_ratio, '图4: 全市场多空比 + 价格')
    create_figure(df, plot5_basis, '图5: 基差、基差率和价格')
    create_figure(df, plot6_funding_rate, '图6: 资金费率和收盘价')
    create_figure(df, plot7_position_value_price, '图7: 持仓市值比与收盘价')

    # 使用非阻塞模式显示图表
    print("\n✓ 已生成7张图表窗口")
    print("提示: 在所有图表中将鼠标放在数据线上可在右下角查看详细数据(3位有效数字)")
    print("注意: 在进行计算时,图表交互功能仍然可用")
    
    # 使用非阻塞模式显示图表
    plt.ion()  # 开启交互模式
    plt.show()
    
    # 添加事件循环处理,确保图表保持响应
    import matplotlib
    matplotlib.use('TkAgg')  # 确保使用支持交互的后端
    
    # 循环询问是否需要计算时间段平均值
    while True:
        print("\n" + "=" * 60)
        print("请选择计算选项:")
        print("1. 计算特定时间段的平均基差和基差率")
        print("2. 计算特定时间段的平均资金费率")
        print("3. 退出")
        print("=" * 60)
        
        choice = input("请输入选项(1/2/3): ").strip()
        
        if choice == '1':
            calculate_period_average(df)
        elif choice == '2':
            calculate_period_funding_rate(df)
        elif choice == '3':
            break
        else:
            print("✗ 无效选项,请输入1、2或3")
    
    return df



# 主程序
if __name__ == "__main__":
    excel_file = get_excel_file()
    
    if excel_file is None:
        print("\n程序已退出")
        exit()
    
    print(f"\n正在读取文件: {excel_file}")
    
    try:
        df = pd.read_excel(excel_file)
        print(f"✓ 成功读取文件,共 {len(df)} 行数据")
        
        if not validate_columns(df):
            print("\n✗ 列名验证失败,程序退出")
            exit()
        
        print("✓ 列名验证通过")
        print("\n开始生成图表...")
        
        plot_futures_analysis(excel_file)
        
        print("\n" + "=" * 60)
        print("✓ 分析完成!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ 发生错误: {e}")
        import traceback
        traceback.print_exc()
    
    input("\n按Enter键退出...")