module WithAdvisoryLock
  class PostgreSQL < Base
    # See http://www.postgresql.org/docs/9.1/static/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
    def try_lock
      if connection.open_transactions > 0
        execute_successful?('pg_try_advisory_xact_lock')
      else
        execute_successful?('pg_try_advisory_lock')
      end
    end

    def try_shared_lock
      if connection.open_transactions > 0
        execute_successful?('pg_try_advisory_xact_lock_shared')
      else
        execute_successful?('pg_try_advisory_lock_shared')
      end
    end

    def exclusive_lock
      if connection.open_transactions > 0
        execute_successful?('pg_advisory_xact_lock')
      else
        execute_successful?('pg_advisory_lock')
      end
    end

    def release_lock
      if connection.open_transactions > 0
        # lock is released automatically at transaction close
        true
      else
        begin
          execute_successful?('pg_advisory_unlock')
        rescue ActiveRecord::StatementInvalid => e
          raise unless e.message =~ / ERROR: +current transaction is aborted,/
          begin
            connection.rollback_db_transaction
            execute_successful?('pg_advisory_unlock')
          ensure
            connection.begin_db_transaction
          end
        end
      end
    end

    def release_shared_lock
      if connection.open_transactions > 0
        # lock is released automatically at transaction close
        true
      else
        begin
          execute_successful?('pg_advisory_unlock_shared')
        rescue ActiveRecord::StatementInvalid => e
          raise unless e.message =~ / ERROR: +current transaction is aborted,/
          begin
            connection.rollback_db_transaction
            execute_successful?('pg_advisory_unlock')
          ensure
            connection.begin_db_transaction
          end
        end
      end
    end

    def execute_successful?(pg_function)
      sql = "SELECT #{pg_function}(#{lock_keys.join(',')})::text AS #{unique_column_name}"
      result = connection.select_value(sql)
      # MRI returns 't', jruby returns true. YAY!
      (result == 't' || result == '' || result == true)
    end

    # PostgreSQL wants 2 32bit integers as the lock key.
    def lock_keys
      @lock_keys ||= begin
        [stable_hashcode(lock_name), ENV['WITH_ADVISORY_LOCK_PREFIX']].map do |ea|
          # pg advisory args must be 31 bit ints
          ea.to_i & 0x7fffffff
        end
      end
    end
  end
end

