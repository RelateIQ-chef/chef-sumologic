require 'net/https'
require 'uri'
require 'json'

class Chef
  class Provider
    class SumoSource < Chef::Provider
      def initialize(new_resource, run_context)
        super(new_resource, run_context)
      end

      def whyrun_supported?
        true
      end

      def load_current_resource
        return if node['sumologic']['disabled']
        @@collector ||= Sumologic::Collector.new(
          :name => node.name,
          :api_username => node['sumologic']['userID'],
          :api_password => node['sumologic']['password'],
          :api_endpoint => node['sumologic']['api_endpoint'],
          :api_collectors_limit => node['sumologic']['api_collectors_limit']
        )

        @current_resource = Chef::Resource::SumoSource.new(@new_resource.name)

        @current_resource.path(@new_resource.path)
        @current_resource.category(@new_resource.category)
        @current_resource.default_timezone(@new_resource.default_timezone)
        @current_resource.force_timezone(@new_resource.force_timezone)
        @current_resource.blacklist(@new_resource.blacklist)
        if @@collector.source_exist?(@new_resource.name) && (!node['sumologic']['disabled'])
          resource_hash = @@collector.source(@new_resource.name)
          @current_resource.path(resource_hash['pathExpression'])
          @current_resource.default_timezone(resource_hash['timeZone'])
          @current_resource.force_timezone(resource_hash['forceTimeZone'])
          @current_resource.category(resource_hash['category'])
          @current_resource.blacklist(resource_hash['blacklist'])
        end
        @current_resource
      end

      def action_create
        if node['sumologic']['disabled']
          Chef::Log.debug('Skipping sumo source declaration as sumologic::disabled is set to true')
        else
          if @@collector.source_exist?(new_resource.name)
            if sumo_source_different?
              converge_by("replace #{new_resource.name} via api\n" + convergence_description) do
                @@collector.update_source!(@@collector.source(new_resource.name)['id'], new_resource.to_sumo_hash)
                @@collector.refresh!
              end
              @new_resource.updated_by_last_action(true)
              Chef::Log.info("#{@new_resource} replaced sumo_source entry")
            else
              # resource is present and its same as specified
            end
          else
            converge_by("add #{new_resource.name} via sumologic api\n" + new_resource.to_sumo_hash.to_s)  do
              @@collector.add_source!(new_resource.to_sumo_hash)
              @@collector.refresh!
            end
            @new_resource.updated_by_last_action(true)
            Chef::Log.info("#{@new_resource} added sumo_source entry")
          end
        end
      end

      def action_delete
        if node['sumologic']['disabled']
          Chef::Log.debug('Skipping sumo source declaration as sumologic::disabled is set to true')
        else
          if @@collector.source_exist?(@new_resource.name)
            converge_by "removing sumo source #{@new_resource.name}" do
              raise ArgumentError, 'Not implemented yet'
            end
            @new_resource.updated_by_last_action(true)
            Chef::Log.info("#{@new_resource} deleted sumo_source entry")
          end
        end
      end

      private

      def sumo_source_different?
        Chef::Resource::SumoSource.state_attrs.any? do |attr|
          @current_resource.send(attr) != @new_resource.send(attr)
        end
      end

      def convergence_description
        description = ''
        Chef::Resource::SumoSource.state_attrs.each do |attr|
          current_value = @current_resource.send(attr)
          new_value = @new_resource.send(attr)
          if current_resource != new_value
            description << "value of #{attr} will change from '#{current_value}' to '#{new_value}'\n"
          end
        end
        description
      end
    end
  end
end
