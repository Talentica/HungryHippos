require 'spec_helper'

describe 'hadoop::hadoop_hdfs_secondarynamenode' do
  context 'on Centos 6.6' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        node.default['hadoop']['hdfs_site']['dfs.namenode.checkpoint.dir'] = '/tmp/hadoop-hdfs/dfs/namesecondary'
        node.default['hadoop']['hdfs_site']['dfs.namenode.checkpoint.edits.dir'] = '/tmp/hadoop-hdfs/dfs/namesecondaryedits'
        stub_command(/update-alternatives --display /).and_return(false)
        stub_command(%r{/sys/kernel/mm/(.*)transparent_hugepage/defrag}).and_return(false)
        stub_command(/test -L /).and_return(false)
      end.converge(described_recipe)
    end
    pkg = 'hadoop-hdfs-secondarynamenode'

    %W(
      /etc/default/#{pkg}
      /etc/init.d/#{pkg}
    ).each do |file|
      it "creates #{file} from template" do
        expect(chef_run).to create_template(file)
      end
    end

    it "creates #{pkg} service resource, but does not run it" do
      expect(chef_run).to_not disable_service(pkg)
      expect(chef_run).to_not enable_service(pkg)
      expect(chef_run).to_not reload_service(pkg)
      expect(chef_run).to_not restart_service(pkg)
      expect(chef_run).to_not start_service(pkg)
      expect(chef_run).to_not stop_service(pkg)
    end

    it 'creates HDFS checkpoint dirs' do
      expect(chef_run).to create_directory('/tmp/hadoop-hdfs/dfs/namesecondary').with(
        user: 'hdfs',
        group: 'hdfs'
      )
      expect(chef_run).to create_directory('/tmp/hadoop-hdfs/dfs/namesecondaryedits').with(
        user: 'hdfs',
        group: 'hdfs'
      )
    end
  end
end
