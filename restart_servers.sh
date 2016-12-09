ps aux | grep -ie "python server.py" | awk '{print $2}' | xargs kill

nohup python server.py --host_address=104.155.228.39 --port=8080 --proxy_80_port=poke-server1-8080.getsamosa.com --force=True --log_level=debug &

nohup python server.py --host_address=104.155.228.39 --port=8081 --proxy_80_port=poke-server1-8081.getsamosa.com --force=True --log_level=debug &

nohup python server.py --host_address=104.155.228.39 --port=8082 --proxy_80_port=poke-server1-8082.getsamosa.com --force=True --log_level=debug &

nohup python server.py --host_address=104.155.228.39 --port=8083 --proxy_80_port=poke-server1-8083.getsamosa.com --force=True --log_level=debug &

nohup python server.py --host_address=104.155.228.39 --port=8084 --proxy_80_port=poke-server1-8084.getsamosa.com --force=True --log_level=debug &

nohup python server.py --host_address=104.155.228.39 --port=8085 --proxy_80_port=poke-server1-8085.getsamosa.com --force=True --log_level=debug &

nohup python server.py --host_address=104.155.228.39 --port=8086 --proxy_80_port=poke-server1-8086.getsamosa.com --force=True --log_level=debug &

nohup python server.py --host_address=104.155.228.39 --port=8087 --proxy_80_port=poke-server1-8087.getsamosa.com --force=True --log_level=debug &

nohup python server.py --host_address=104.155.228.39 --port=8088 --proxy_80_port=poke-server1-8088.getsamosa.com --force=True --log_level=debug &