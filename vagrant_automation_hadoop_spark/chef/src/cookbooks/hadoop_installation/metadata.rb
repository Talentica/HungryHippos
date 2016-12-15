name             'hadoop_installation'
maintainer       'YOUR_COMPANY_NAME'
maintainer_email 'YOUR_EMAIL'
license          'All rights reserved'
description      'Installs/Configures hadoop_installation'
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          '0.1.0'

depends "apt"
depends "HH_java"
depends "create_group_user"
depends "disable_IPV6"
depends "download_hadoop"
depends "hadoop_conf_files_setup"
#depends "hadoop_ssh_keygen_trans_mastertoslaves"
