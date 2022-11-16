import asyncio
import contextlib

from hat import aio
import hat.event.server.backends.lmdb


ordered = [
    {"order_by": "TIMESTAMP", "subscriptions": [["event", "engine"]]},
    {"order_by": "TIMESTAMP", "subscriptions": [["gui", "eds", "system", "list", "record", "?"]]},
    {"order_by": "TIMESTAMP", "subscriptions": [["gui", "eds", "system", "list", "soe", "?"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_10_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_8_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_12_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_11_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_9_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_22_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_20_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_24_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_23_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_21_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_34_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_32_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_35_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_33_group_1_day_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_5s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_5s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_5s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_15s_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_15s_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_15s_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_1m_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_1m_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_1m_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_1h_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_1h_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_1h_avg"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_day_min"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_day_max"]]},
    {"order_by": "SOURCE_TIMESTAMP", "subscriptions": [["gui", "eds", "system", "timeseries", "sampled_point_36_group_1_day_avg"]]},
    {"order_by": "TIMESTAMP", "subscriptions": [["*"]]}
]

conf = {'db_path': 'data/event.db',
        'max_db_size': 512 * 1024 * 1024 * 1024,
        'flush_period': 100000,
        'conditions': [],
        'latest': {'subscriptions': [['*']]},
        'ordered': [{**i, 'limit': {'max_entries': 0}}
                    for i in ordered]}


def main():
    with contextlib.suppress(asyncio.CancelledError):
        aio.run_asyncio(async_main())


async def async_main():
    backend = await hat.event.server.backends.lmdb.create(conf)
    await backend.async_close()


if __name__ == '__main__':
    main()
