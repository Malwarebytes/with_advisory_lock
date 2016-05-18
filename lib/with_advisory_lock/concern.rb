require 'active_support/concern'

module WithAdvisoryLock
  module Concern
    extend ActiveSupport::Concern
    delegate :with_advisory_lock, :advisory_lock_exists?, to: 'self.class'

    module ClassMethods
      def with_advisory_lock(lock_name, timeout_seconds=nil, &block)
        Rails.logger.debug "[WithAdvisoryLock]#with_advisory_lock lock_name: #{ lock_name }"
        result = with_advisory_lock_result(lock_name, timeout_seconds, &block)
        result.lock_was_acquired? ? result.result : false
      end

      def with_advisory_lock_result(lock_name, timeout_seconds=nil, &block)
        impl = impl_class.new(connection, lock_name, timeout_seconds)
        impl.with_advisory_lock_if_needed(&block)
      end

      def advisory_lock_exists?(lock_name)
        Rails.logger.debug "[WithAdvisoryLock]#advisory_lock_exists? lock_name: #{ lock_name }"
        impl = impl_class.new(connection, lock_name, 0)
        impl.already_locked? || !impl.yield_with_lock.lock_was_acquired?
      end

      def current_advisory_lock
        WithAdvisoryLock::Base.lock_stack.first
      end

      def try_acquire_lock!(lock_name, timeout_seconds = nil)
        Rails.logger.debug "[WithAdvisoryLock]#acquire_lock! lock_name: #{ lock_name }"
        impl = impl_class.new(connection, lock_name, timeout_seconds)
        impl.try_lock
      end

      def try_acquire_shared_lock!(lock_name, timeout_seconds = nil)
        if impl_class == WithAdvisoryLock::PostgreSQL
          Rails.logger.debug "[WithAdvisoryLock]#try_acquire_shared_lock! lock_name: #{ lock_name }"
          impl = impl_class.new(connection, lock_name, timeout_seconds)
          impl.try_shared_lock
        else
          try_acquire_lock!(lock_name, timeout_seconds)
        end
      end

      def try_release_lock!(lock_name)
        Rails.logger.debug "[WithAdvisoryLock]#release_lock! lock_name: #{ lock_name }"
        impl = impl_class.new(connection, lock_name, 0)
        impl.release_lock
      end

      def try_release_shared_lock!(lock_name)
        if impl_class == WithAdvisoryLock::PostgreSQL
          Rails.logger.debug "[WithAdvisoryLock]#try_release_shared_lock! lock_name: #{ lock_name }"
          impl = impl_class.new(connection, lock_name, 0)
          impl.release_shared_lock
        else
          try_release_lock!(lock_name)
        end
      end

      private

      def impl_class
        adapter = WithAdvisoryLock::DatabaseAdapterSupport.new(connection)
        if adapter.postgresql?
          WithAdvisoryLock::PostgreSQL
        elsif adapter.mysql?
          WithAdvisoryLock::MySQL
        else
          WithAdvisoryLock::Flock
        end
      end
    end
  end
end
