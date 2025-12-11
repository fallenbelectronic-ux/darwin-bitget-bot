#!/usr/bin/env bash
# exit on error
set -o errexit

# Installer TA-Lib (bibliothèque C)
cd /tmp
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib/
./configure --prefix=/opt/render/project/src/.apt/usr
make
make install
cd ~

# Configurer les chemins pour TA-Lib
export LD_LIBRARY_PATH=/opt/render/project/src/.apt/usr/lib:$LD_LIBRARY_PATH
export TA_LIBRARY_PATH=/opt/render/project/src/.apt/usr/lib
export TA_INCLUDE_PATH=/opt/render/project/src/.apt/usr/include

# Installer les dépendances Python
pip install --upgrade pip
pip install -r requirements.txt
