# Dockerfile
FROM hyperpilot/influx:1.2.2
RUN apt-get update
# RUN apt-get install -y build-essential autoconf libtool pkg-config python-opengl python-imaging python-pyrex python-pyside.qtopengl idle-python2.7 qt4-dev-tools qt4-designer libqtgui4 libqtcore4 libqt4-xml libqt4-test libqt4-script libqt4-network libqt4-dbus python-qt4 python-qt4-gl libgle3 python-dev libssl-dev  
RUN apt-get install -y python-dev build-essential libssl-dev libffi-dev libxml2-dev libxslt1-dev zlib1g-dev python-pip libpq-dev  libldap2-dev libsasl2-dev libgnutls28-dev libcurl4-gnutls-dev
RUN pip install pytest
RUN rm -rf /var/lib/apt/lists/*