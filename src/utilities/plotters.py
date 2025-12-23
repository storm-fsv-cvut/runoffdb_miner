import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates

from .utilities import format_timedelta_min

def plot_series_to_file(df, series_names, file_path, title=None, xlabel = None, ylabels=None, legend=True):
    """
    Plot multiple series from a DataFrame and save the plot to a PNG image file.

    :param df: DataFrame, Input DataFrame containing the series to be plotted.
    :param series_names: list of str, List of series names to be plotted.
    :param file_path: str, File path to save the plot as a PNG image.
    :param title: str, optional, Title of the plot.
    :param xlabel: str, optional, Label for the x-axis.
    :param ylabels: list of str, optional, Labels for the y-axes
    :param legend: bool, optional, Whether to display the legend. Default is True.
    """
    from pandas import Timedelta

    # Clear existing plot
    plt.clf()

    # Plot each series with separate y-axis
    fig, ax1 = plt.subplots()
    if xlabel:
        ax1.set_xlabel(xlabel)
    ax1.set_ylabel(ylabels[0] or series_names[0], color='tab:blue')
    ax1.plot(df.index, df[series_names[0]], color='tab:blue', label=ylabels[0] or series_names[0])

    i = 1
    for series_name in series_names[1:]:
        ax2 = ax1.twinx()
        ax2.set_ylabel(series_name if ylabels[i] is None else ylabels[i], color='tab:red')
        ax2.plot(df.index, df[series_name], color='tab:red', label=series_name)

    # Set title
    if title:
        plt.title(title)

    # Remove border around the plot
    # ax1.spines['top'].set_visible(False)
    # ax1.spines['right'].set_visible(False)
    # ax1.spines['bottom'].set_visible(False)
    # ax1.spines['left'].set_visible(False)
    # ax1.tick_params(axis='both', which='both', length=0)  # Remove tick marks

    # Set axes properties
    plt.axhline(0, color='black', linewidth=1)
    plt.axvline(0, color='black', linewidth=1)

    # Create a custom timedelta formatter
    timedelta_formatter = ticker.FuncFormatter(lambda x, pos: format_timedelta_min(Timedelta(x)))

    # Set the formatter for the x-axis
    ax1.xaxis.set_major_formatter(timedelta_formatter)
    ax1.tick_params(axis='x', rotation=90)  # Rotate x-axis labels

    # Set labels
    if xlabel:
        plt.xlabel(xlabel)

    # Add legend if specified
    if legend:
        plt.legend()

    # Save the plot to a PNG file
    plt.savefig(file_path)

def plot_flow_comparison(df_original, integrated_results, value_col, duration_col, title=None, output=None):
    """
    Plot sample volumes (bars), flow rates (lines), and cumulative discharges (lines) for multiple placement cases.

    Parameters
    ----------
    df_original : pandas.DataFrame
        Original data with TimedeltaIndex (sampling START times), containing raw sample volumes and durations.
    integrated_results : dict
        {case_label: integrated_df}, where integrated_df has 'flow_rate' and 'cum_discharge' columns
        and an index with the derived timestamps for that case.
    value_col : str
        Column name for sample volume (L).
    duration_col : str
        Column name for sampling duration (s).
    title : str, optional
        Plot title.
    output : str, optional
        Path to save the figure instead of showing it.
    """
    from pandas import Timestamp, to_timedelta

    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Reference datetime so TimedeltaIndex can be plotted with hh:mm:ss
    zero_time = Timestamp("2000-01-01")
    original_times = zero_time + df_original.index               # sampling START
    case_times = {case: zero_time + df.index for case, df in integrated_results.items()}

    # --- Left Y axis: sample volumes as bars starting at sampling start ---
    ax1.bar(
        original_times,
        df_original[value_col],
        width=to_timedelta(df_original[duration_col], unit='s'),  # full duration
        align='edge',                                                # left edge at start time
        color='lightblue',
        label='sample volume (l)'
    )
    ax1.set_xlabel("time from start (hh:mm:ss)")
    ax1.set_ylabel("sample volume (l)", color='black')
    ax1.tick_params(axis='y', labelcolor='black')

    # X axis formatting
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())

    # --- Right Y axis #1: flow rates ---
    ax2 = ax1.twinx()
    flow_colors = {'start': 'orange', 'sample_mid': 'red', 'interval_mid': 'purple'}
    for case, df in integrated_results.items():
        ax2.plot(
            case_times[case],
            df['flow_rate'],
            color=flow_colors.get(case, None),
            marker='o',
            label=f"flow rate ({case})",
            zorder=3
        )
    ax2.set_ylabel("flow rate (l/s)")
    ax2.tick_params(axis='y', labelcolor='black')

    # --- Right Y axis #2: cumulative discharge (same linestyle ":" for all) ---
    ax3 = ax1.twinx()
    ax3.spines["right"].set_position(("outward", 60))
    for case, df in integrated_results.items():
        ax3.plot(
            case_times[case],
            df['cum_discharge'],
            linestyle=':',
            color=flow_colors.get(case, None),
            marker='s',
            label=f"cumulative discharge ({case})",
            zorder=2
        )
    ax3.set_ylabel("cumulative discharge (l)")
    ax3.tick_params(axis='y', labelcolor='black')

    # Scale both right-side axes independently (keeps both readable)
    flow_min = min(d['flow_rate'].min() for d in integrated_results.values())
    flow_max = max(d['flow_rate'].max() for d in integrated_results.values())
    ax2.set_ylim(flow_min * 0.9, flow_max * 1.1)

    cd_min = min(d['cum_discharge'].min() for d in integrated_results.values())
    cd_max = max(d['cum_discharge'].max() for d in integrated_results.values())
    ax3.set_ylim(cd_min * 0.9 if cd_min != 0 else 0, cd_max * 1.1)

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    lines3, labels3 = ax3.get_legend_handles_labels()
    ax1.legend(lines1 + lines2 + lines3,
               labels1 + labels2 + labels3,
               loc='lower right')

    if title:
        ax1.set_title(title)

    plt.tight_layout()
    if output:
        plt.savefig(output, dpi=200)
        plt.close(fig)
    else:
        plt.show()


def plot_frequency_analysis(overview_df, output_path=None):
    """
    draw frequency analysis of the 'sample_mid_diff' and 'sample_end_diff' columns

    parameters
    ----------
    overview_df : pd.DataFrame
        dataframe with 'sample_mid_diff' and 'sample_end_diff' columns
    output_dir : str or None
        if provided, saves the plot as 'overview_diff_frequency.png' in this directory
    """

    plt.figure(figsize=(8, 6))

    # histogram with density (normalized frequencies)
    overview_df["sample_mid_diff"].plot(
        kind="hist", bins=15, alpha=0.5, density=True,
        label="sample_mid_diff"
    )
    overview_df["sample_end_diff"].plot(
        kind="hist", bins=15, alpha=0.5, density=True,
        label="sample_end_diff"
    )

    # overlay kde (smooth density curve)
    overview_df["sample_mid_diff"].plot(
        kind="kde", lw=2, label="sample_mid_diff KDE"
    )
    overview_df["sample_end_diff"].plot(
        kind="kde", lw=2, label="sample_end_diff KDE"
    )

    plt.xlabel("relative difference (1 - case / interval_start)")
    plt.ylabel("frequency (density)")
    plt.title("Frequency analysis of final discharge differences")
    plt.legend()

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300)
        print(f"\tfrequency analysis plot saved to: {output_path}")
    else:
        plt.show()


def plot_hydro_data(df, output_path, series_to_plot=None, xaxis_format="%M:%S",
                    extra_points=None, plot_title=None):
    """
    Plot selected hydro-sediment data and save to file.
    Supports optional extra points per series.

    :param df: DataFrame with hydro-sediment data, indexed by TimedeltaIndex
    :param output_path: File path to save the plot
    :param series_to_plot: Dict {column_name: chart_type} e.g. 'line', 'bar', 'point', 'step'
    :param xaxis_format: X-axis label format (strftime-style, e.g. "%M:%S")
    :param extra_points: Dict {column_name: [(Timedelta, value), ...]}
    :param plot_title: Optional title for the plot
    """

    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MultipleLocator, FuncFormatter

    if df is None or df.empty:
        print("No hydro-sediment data available to plot.")
        return

    if not isinstance(df.index, pd.TimedeltaIndex):
        raise ValueError("DataFrame index must be a TimedeltaIndex.")

    # default chart types
    default_series = {
        'rainfall_intensity': 'bar',
        'rainfall_total': 'line',
        'runoff': 'line',
        'discharge': 'line',
        'sediment_flux': 'line',
        'sediment_yield': 'line'
    }

    # filter what to plot
    if series_to_plot is None:
        series_to_plot = {k: v for k, v in default_series.items() if k in df.columns}
    else:
        series_to_plot = {k: v for k, v in series_to_plot.items() if k in df.columns}

    if not series_to_plot:
        print("No valid columns selected for plotting.")
        return

    # setup figure
    fig, ax1 = plt.subplots(figsize=(14, 6))
    base_ax = ax1
    axes = [base_ax]
    lines = []

    # spacing between right-side Y-axes
    yaxis_spacing = 0.07

    # create additional Y-axes if needed
    for i in range(1, len(series_to_plot)):
        new_ax = base_ax.twinx()
        pos = 1 + yaxis_spacing * (i - 1)
        new_ax.spines["right"].set_position(("axes", pos))
        new_ax.spines["right"].set_visible(True)
        new_ax.yaxis.set_ticks_position('right')
        new_ax.yaxis.set_label_position('right')
        new_ax.patch.set_visible(False)
        axes.append(new_ax)

    if len(series_to_plot) > 1:
        fig.subplots_adjust(right=0.80 + (len(series_to_plot) - 2) * 0.06)

    # --- X-axis setup ---
    # use numeric values in minutes
    x_values = df.index.total_seconds() / 60
    color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']

    # --- plot each series ---
    y_max_quotient = 1.0
    for i, (col, chart_type) in enumerate(series_to_plot.items()):
        ax = axes[i]
        color = color_cycle[i % len(color_cycle)]

        if chart_type == 'bar':
            bar_width = (x_values[1] - x_values[0]) if len(x_values) > 1 else 1
            line = ax.bar(x_values, df[col], width=bar_width, label=col, color=color, alpha=0.6)
        elif chart_type == 'point':
            line, = ax.plot(x_values, df[col], 'o', label=col, color=color)
        elif chart_type == 'step':
            line, = ax.step(x_values, df[col], where='post', label=col, color=color)
        else:  # line
            line, = ax.plot(x_values, df[col], label=col, color=color)

        # optional extra points
        if extra_points and col in extra_points:
            x_extra = [t.total_seconds() / 60 for t, _ in extra_points[col]]
            y_extra = [v for _, v in extra_points[col]]
            ax.plot(x_extra, y_extra, 'o', color='black', markersize=6,
                    markeredgecolor=color, zorder=10)

        # ensure Y-axis starts at 0 and extend top slightly
        ax.relim()
        ax.autoscale_view()
        ymin, ymax = ax.get_ylim()
        ymax *= y_max_quotient
        ax.set_ylim(bottom=0, top=ymax)
        y_max_quotient += 0.05  # next axis slightly taller

        # tighten label spacing
        ax.yaxis.labelpad = 4

        ax.set_ylabel(col, color=color)
        ax.tick_params(axis='y', labelcolor=color)
        lines.append(line)

    # --- X-axis formatting (exact 1-minute ticks starting at 0) ---
    tick_interval = 1.0  # minutes
    base_ax.set_xlim(left=0)

    base_ax.xaxis.set_major_locator(MultipleLocator(tick_interval))
    base_ax.xaxis.set_major_formatter(FuncFormatter(
        lambda x, _: f"{int(x):02d}:{int((x % 1) * 60):02d}"
    ))

    base_ax.set_xlabel("Time [MM:SS]")
    base_ax.grid(True, which='major', axis='x', linestyle='--', alpha=0.4)
    base_ax.grid(True, which='both', axis='y', linestyle='--', alpha=0.3)

    # rotate tick labels
    plt.setp(base_ax.get_xticklabels(), rotation=90, ha='center')

    # --- legend ---
    labels = [l.get_label() if hasattr(l, 'get_label') else l[0].get_label() for l in lines]
    base_ax.legend(lines, labels, loc='lower right', frameon=False)

    plt.title(plot_title or "Hydro-Sediment Time Series")
    plt.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)