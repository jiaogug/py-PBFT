#!/bin/sh
# tmux new-session
tmux split-window -h 'bash -c "python ./proxy_evil.py 1"'
tmux split-window -v 'bash -c "python ./server.py 3 1000 & python client.py 3"'
tmux select-pane -U
tmux split-window -h 'bash -c "python ./server.py 2 1000 & python client.py 2"'
tmux select-pane -D
tmux split-window -h 'bash -c "python ./server.py 1 1000 & python client.py 1"'
tmux select-pane -L
tmux split-window -v 'bash -c "python ./server.py 0 1000 & python client.py 0"'
# trap 'killall python' INT TERM EXIT