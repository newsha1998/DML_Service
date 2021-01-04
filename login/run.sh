tmux kill-session -t runserver
sleep 1
tmux new-session -d -s runserver
tmux send-keys -t runserver 'cd ~/Desktop/DML_Service/login' ENTER
tmux send-keys -t runserver 'python3 manage.py runserver $1' ENTER
