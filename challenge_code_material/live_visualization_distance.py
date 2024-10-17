from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pandas as pd
import threading
from collections import defaultdict, deque
import matplotlib.dates as mdates
import time
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter
from datetime import datetime, time as datetime_time
import matplotlib.image as mpimg

conf = {
    'bootstrap.servers': 'kafka-1:19092',
    'group.id': 'challenge6',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topics = ["Challenge6RunnerDistanceComparisonAdd"]

running = True

max_points = 9000  # Number of data points to display


# Use deque with maxlen for automatic data discarding
distance_data = defaultdict(lambda: deque(maxlen=max_points))
time_data = defaultdict(lambda: deque(maxlen=max_points))
data_lock = threading.Lock()

# Variables to store the latest runner and rabbit data
runner_data_latest = {}
rabbit_data_latest = {}

def format_y_tick(value, tick_number):
    return f'{value:.0f} m'

# Create the figure and define subplots with gridspec
fig = plt.figure(figsize=(10, 3))
gs = gridspec.GridSpec(1, 3, width_ratios=[1, 3, 1])

ax_runner = plt.subplot(gs[0])
ax_main = plt.subplot(gs[1])
ax_rabbit = plt.subplot(gs[2])

fig.patch.set_facecolor('#D9D9D9')
ax_main.set_facecolor('#D9D9D9')

icon = mpimg.imread('finish_icon.png')
# Define the start time (08:45)
target_time = datetime.combine(datetime(2024, 10, 6).date(), datetime_time(hour=8, minute=45))

def animate(i):
    with data_lock:
        # Clear the axes
        ax_runner.clear()
        ax_main.clear()
        ax_rabbit.clear()

        
        # Plot the distance data over time on the main axis
        all_plot_times = []

        for runner_key in distance_data:
            plot_times = list(time_data[runner_key])
            plot_distances = list(distance_data[runner_key])

            if plot_times and plot_distances:
                ax_main.plot(plot_times, plot_distances, label=f'Runner {runner_key}', linewidth=3,color='teal')
                all_plot_times.extend(plot_times)
                

        # ax_main.set_ylabel('meters')
        ax_main.set_title('Distance between Runner and Rabbit', fontsize=14)

        if all_plot_times:
            min_time = min(all_plot_times)
            max_time = max(all_plot_times)
            ax_main.set_xlim(min_time, max_time)

            # Plot the vertical line if target_time is within x-axis limits
            if min_time <= target_time <= max_time:
                # Get current y-axis limits
                ymin, ymax = ax_main.get_ylim()
                ax_main.vlines(target_time, ymin, ymax, colors='black', linestyles='dashed', linewidth=2)


            ax_main.text(0.05, 0.95, f'{plot_distances[-1]:0.0f} m', transform=ax_main.transAxes,
                    fontsize=14, fontweight='bold', verticalalignment='top', horizontalalignment='left', 
                    color='teal')
        

        ax_main.yaxis.set_major_formatter(FuncFormatter(format_y_tick))
        ax_main.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax_main.xaxis.set_major_locator(mdates.AutoDateLocator())

        ax_main.axhline(y=0, color='gray', linestyle='-', linewidth=1)

        # handles, labels = ax_main.get_legend_handles_labels()
        # if labels:
        #     ax_main.legend(loc='upper left', fontsize=8)

        if runner_data_latest:
            ax_runner.set_title(' ', fontsize=14)

            # Get the speed and cumulative distance
            speed = runner_data_latest.get('speed', 0)
            cumulative_distance = runner_data_latest.get('cumulative_distance', 0) / 1000  # Convert to km

            # Position parameters
            x_position = 0.5
            y_speed = 0.6
            y_distance = 0.4

            # Display speed
            ax_runner.text(x_position, y_speed, f'{speed:.1f}',
                        fontsize=22, fontweight='bold', ha='right', va='center', transform=ax_runner.transAxes)
            ax_runner.text(x_position, y_speed, ' km/h',
                        fontsize=12, ha='left', va='center', transform=ax_runner.transAxes)

            # Display cumulative distance
            ax_runner.text(x_position, y_distance, f'{cumulative_distance:.1f}',
                        fontsize=22, fontweight='bold', ha='right', va='center', transform=ax_runner.transAxes)
            ax_runner.text(x_position, y_distance, ' km',
                        fontsize=12, ha='left', va='center', transform=ax_runner.transAxes)
            
            if cumulative_distance >= 17.2:
                ax_runner.imshow(icon, extent=[0.4, 0.6, 0.1, 0.3], transform=ax_runner.transAxes, aspect='auto')

            ax_runner.axis('off')


        # Update rabbit data visualization on the right axis
        if rabbit_data_latest:
            ax_rabbit.set_title(' ', fontsize=14)

            # Get the speed and cumulative distance
            speed = rabbit_data_latest.get('speed', 0)
            cumulative_distance = rabbit_data_latest.get('cumulative_distance', 0) / 1000  # Convert to km

            # Position parameters
            x_position = 0.5
            y_speed = 0.6
            y_distance = 0.4

            # Display speed
            ax_rabbit.text(x_position, y_speed, f'{speed:.1f}',
                        fontsize=22, fontweight='bold', ha='right', va='center', transform=ax_rabbit.transAxes)
            ax_rabbit.text(x_position, y_speed, ' km/h',
                        fontsize=12, ha='left', va='center', transform=ax_rabbit.transAxes)

            # Display cumulative distance
            ax_rabbit.text(x_position, y_distance, f'{cumulative_distance:.1f}',
                        fontsize=22, fontweight='bold', ha='right', va='center', transform=ax_rabbit.transAxes)
            ax_rabbit.text(x_position, y_distance, ' km',
                        fontsize=12, ha='left', va='center', transform=ax_rabbit.transAxes)
            
            if cumulative_distance >= 17.2:
                ax_rabbit.imshow(icon, extent=[0.4, 0.6, 0.1, 0.3], transform=ax_rabbit.transAxes, aspect='auto')

            ax_rabbit.axis('off')


        plt.tight_layout()

        # Save the figure as an image to create give for presentation
        # fig.savefig(f'animation/live_visualization_distance_{i:04d}.png')

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            # time.sleep(0.001) #only for demo purposes
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
                continue

            key = msg.key().decode("utf-8") if msg.key() else 'default_key'
            value = json.loads(msg.value().decode("utf-8"))

            with data_lock:
                # Extract distance and timestamp
                distance = value.get('distance_between_runners')
                timestamp = pd.to_datetime(value['timestamp'], unit='s')

                # Update distance data
                if distance is not None:
                    distance_data[key].append(distance)
                    time_data[key].append(timestamp)

                # Extract runner_data and rabbit_data
                runner_data = value.get('runner_data', {})
                rabbit_data = value.get('rabbit_data', {})

                # Update the latest runner and rabbit data
                runner_data_latest.clear()
                runner_data_latest.update(runner_data)

                rabbit_data_latest.clear()
                rabbit_data_latest.update(rabbit_data)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Start the consumer loop in a separate thread
consumer_thread = threading.Thread(target=basic_consume_loop, args=(consumer, topics))
consumer_thread.start()

# Start the animation
ani = animation.FuncAnimation(fig, animate, interval=100)
plt.show()

# Wait for the consumer thread to finish
consumer_thread.join()
