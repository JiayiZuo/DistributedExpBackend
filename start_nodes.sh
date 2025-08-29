#!/bin/bash

# 清理之前的存储
rm -rf raft snapshots

# 创建存储目录
mkdir -p raft snapshots

# 启动第一个节点 (引导节点)
echo "Starting node 1..."
./distributedExp.exe node1 localhost:10001 > node1.log 2>&1 &

# 等待第一个节点启动
sleep 3

# 启动第二个节点
echo "Starting node 2..."
./distributedExp.exe node2 localhost:10002 localhost:10001 > node2.log 2>&1 &

# 启动第三个节点
echo "Starting node 3..."
./distributedExp.exe node3 localhost:10003 localhost:10001 > node3.log 2>&1 &

echo "All nodes started. Logs are in node1.log, node2.log, node3.log"
echo "Access the application at http://localhost:3000, http://localhost:3001, http://localhost:3002"

# 等待用户按下Ctrl+C
echo "Press Ctrl+C to stop all nodes"
wait
