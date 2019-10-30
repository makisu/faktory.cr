require "raven"

module Faktory
  class Worker
    alias OptionsHash = Hash(Symbol, Bool | Int32 | Array(String))

    private class OptionDeck
      @options : OptionsHash

      def initialize
        @options = {
          :shuffle     => true,
          :queues      => ["default"],
          :concurrency => 2,
        }
      end

      def shuffle?(shuffle : Bool) : OptionDeck
        self.tap { @options[:shuffle] = shuffle }
      end

      def queue(*target_queues : String) : OptionDeck
        self.queues(*target_queues)
      end

      def queues(*target_queues : String) : OptionDeck
        self.tap { @options[:queues] = target_queues.to_a }
      end

      def concurrency(max_concurrency : Int32) : OptionDeck
        self.tap { @options[:concurrency] = max_concurrency }
      end

      protected def expose : OptionsHash
        @options
      end
    end

    @consumer : Consumer
    @consumer_mutex : Mutex
    @shuffle : Bool = true
    @queues : Array(String)
    @concurrency : Int32 = 2
    @quiet : Bool = false
    @terminate : Bool = false
    @last_heartbeat : Time = Time.utc
    @running : Bool = false

    def initialize(debug : Bool = false)
      @consumer = Consumer.new
      @consumer_mutex = Mutex.new
      @queues = ["default"]
    end

    def running? : Bool
      @running
    end

    def shutdown!
      @quiet = true
      @terminate = true
    end

    def run
      @quiet = false
      @terminate = false
      @running = true

      @concurrency.times do
        spawn do
          until terminated?
            job = nil
            if quieted?
              sleep 0.5
            else
              job = fetch
            end
            process(job.as(Job)) if job
            @consumer_mutex.synchronize { heartbeat if should_heartbeat? }
          end
        rescue ex : Exception
          puts "Worker crashed! Will restart in 15 seconds."
          puts "Error was:\n#{ex.inspect}"
          if ENV["SENTRY_DSN"]?
            begin
              Raven.capture(ex)
            rescue sentry_ex : Exception
              puts "Could not send event to Sentry!\n#{ex.inspect}"
            end
          end
          sleep 15
          puts "Restarting worker..."
        end
      end

      until terminated?
        sleep 0.5 # just run the worker fibers until killed
      end

      @running = false
    end

    def run(&block : OptionDeck -> OptionDeck)
      option_deck = yield OptionDeck.new
      options = option_deck.expose
      @shuffle = options[:shuffle].as(Bool)
      @queues = options[:queues].as(Array(String))
      @concurrency = options[:concurrency].as(Int32)
      self.run
    end

    private def should_heartbeat? : Bool
      (Time.utc - @last_heartbeat).total_seconds >= 15
    end

    private def heartbeat
      response = @consumer_mutex.synchronize { @consumer.beat }
      if response
        state = response.as(String)
        @quiet = true
        @terminate = true if state == "terminate"
      end
      @last_heartbeat = Time.utc
    end

    private def quieted? : Bool
      @quiet
    end

    private def terminated? : Bool
      @terminate
    end

    private def shuffle? : Bool
      @shuffle
    end

    private def fetch : Job | Nil
      @queues = @queues.shuffle if shuffle?
      job_payload = @consumer_mutex.synchronize { @consumer.fetch(@queues) }
      if job_payload
        Job.deserialize(job_payload.as(JSON::Any))
      else
        nil
      end
    end

    private def process(job : Job)
      Faktory.log.info("START " + job.jid)
      begin
        job.perform
        @consumer_mutex.synchronize { @consumer.ack(job.jid) }
      rescue e
        @consumer_mutex.synchronize { @consumer.fail(job.jid, e) }
      end
      GC.collect # AKN: added manual GC run, memory usage was out of control
    end
  end
end
