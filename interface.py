import altair as alt
import pandas as pd
import gradio as gr
import datetime
import time


def make_plot_1():
    with open('count.csv', 'r', newline='', encoding='utf-8') as f:
        source = pd.read_csv(f)

    highlight = alt.selection(type='single', on='mouseover',
                              fields=['symbol'], nearest=True)

    base = alt.Chart(source).encode(
        x='date:T',
        y='price:Q',
        color='symbol:N'
    )

    points = base.mark_circle().encode(
        opacity=alt.value(0)
    ).add_selection(
        highlight
    ).properties(
        width=600
    )

    lines = base.mark_line().encode(
        size=alt.condition(~highlight, alt.value(1), alt.value(3))
    )

    return points + lines


def make_plot_2():
    with open('count_datetime.csv', 'r', newline='', encoding='utf-8') as f:
        source = pd.read_csv(f)

    highlight = alt.selection(type='single', on='mouseover',
                              fields=['symbol'], nearest=True)

    base = alt.Chart(source).encode(
        x='date:T',
        y='price:Q',
        color='symbol:N'
    )

    points = base.mark_circle().encode(
        opacity=alt.value(0)
    ).add_selection(
        highlight
    ).properties(
        width=600
    )

    lines = base.mark_line().encode(
        size=alt.condition(~highlight, alt.value(1), alt.value(3))
    )

    return points + lines


def elapsed_time():
    while True:
        current_time = datetime.datetime.now()
        elapsed_seconds = current_time - datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f')
        return str(elapsed_seconds)
        time.sleep(5)


with open('start_time.txt', 'r', newline='', encoding='utf-8') as f:
    start_time = f.readline()
    print(start_time)


with gr.Blocks() as demo:
    with gr.Row():
        plot_1 = gr.Plot(label="Plot")
        plot_2 = gr.Plot(label="Plot")

    time_difference = gr.Markdown(elapsed_time)
    # demo.load(time_difference, inputs=elapsed_time, live=True)

    demo.load(make_plot_1, inputs=None, outputs=[plot_1])
    demo.load(make_plot_2, inputs=None, outputs=[plot_2])

demo.queue().launch()