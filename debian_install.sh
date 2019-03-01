#!/usr/bin/env sh

SCRIPT=$(readlink -f "$0")
SCRIPT_PATH=$(dirname "$SCRIPT")

# Fix locale issues
export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"
export LANGUAGE="en_US.UTF-8"
export LC_TIME="en_US-UTF-8"
export LC_MONETARY="en_US-UTF-8"
export LC_ADDRESS="en_US-UTF-8"
export LC_TELEPHONE="en_US.UTF-8"
export LC_NAME="en_US.UTF-8"
export LC_MEASUREMENT="en_US.UTF-8"
export LC_IDENTIFICATION="en_US.UTF-8"
export LC_NUMERIC="en_US.UTF-8"
export LC_PAPER="en_US.UTF-8"

# Install requirements
if which python > /dev/null 2>&1 ; then
	sudo apt-get install autossh ssh-client lsof procps || exit 3
else
	sudo apt-get install autossh ssh-client python3 lsof procps || exit 3
fi

# Configure user
if ! getent passwd ssh-tunnel > /dev/null 2>&1; then
	sudo adduser --system --home /usr/local/sshtunnel ssh-tunnel || exit 4
fi
if ! sudo test -f /usr/local/sshtunnel/.ssh/id_rsa ; then
	sudo su -l ssh-tunnel -s /bin/bash -c "ssh-keygen -t rsa -b 4096 -q -N '' -f /usr/local/sshtunnel/.ssh/id_rsa" || exit 5
fi

# Install main file
sudo cp "$SCRIPT_PATH/src/sshtunnel.py" /usr/local/sshtunnel/sshtunnel.py || exit 6
sudo chmod 755 /usr/local/sshtunnel/sshtunnel.py || exit 11
if ! [ -e /usr/local/bin/sshtunnel ]; then
	sudo ln -s  /usr/local/sshtunnel/sshtunnel.py /usr/local/bin/sshtunnel || exit 7
fi

# Prepare configuration
sudo mkdir -p /usr/local/sshtunnel/servers-available || exit 8
sudo mkdir -p /usr/local/sshtunnel/servers-enabled || exit 9
if ! sudo test -e /usr/local/sshtunnel/sshtunnel.conf ; then
	sudo cp "$SCRIPT_PATH/conf_examples/default/sshtunnel.conf" /usr/local/sshtunnel/ || exit 10
	sudo chmod 644 /usr/local/sshtunnel/sshtunnel.conf
fi

# Ensure ownership
sudo chown -R ssh-tunnel:nogroup /usr/local/sshtunnel || exit 12

# Install the systemd service
if ! [ -e "/etc/systemd/system/sshtunnel@.service" ]; then
	sudo cp "$SCRIPT_PATH/conf_examples/sshtunnel@.service" "/etc/systemd/system/sshtunnel@.service" || exit 13
	sudo chown root:root "/etc/systemd/system/sshtunnel@.service" || exit 14
	sudo chmod 644 "/etc/systemd/system/sshtunnel@.service" || exit 15
	sudo systemctl daemon-reload
fi

# End, display final message
echo ""
echo ""
echo "The ssh-tunnel tool has been successfully deployed."
echo "Now you need to configure it."
echo "Configuration is located in folder /usr/local/sshtunnel/"
echo ""
echo "After configuration, you can enable the tunnels you want this way:"
echo "  systemctl enable ssh-tunnel@my.domain.tld"
echo ""
echo "Thank you for installing, bye :-) "
echo ""
