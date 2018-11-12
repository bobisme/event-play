require "json"
require "uuid"

record EventRecord,
  global_position : UInt64,
  stream_position : UInt64,
  category : String,
  stream_name : String,
  message_type : String,
  metadata : String,
  data : String

record EventObj,
  global_position : UInt64,
  stream_position : UInt64,
  category : String,
  stream_name : String,
  message_type : String,
  metadata : JSON::Any,
  data : JSON::Any

alias EventHandler = EventObj -> Nil

class EventStore
  @store : Array(EventRecord) = [] of EventRecord
  @next_global_position : UInt64 = 0
  @next_stream_positions : Hash(String, UInt64) = {} of String => UInt64

  def write(category : String, stream : String, message_type : String, event)
    now = Time.utc_now
    event_id = UUID.random.to_s
    next_stream_position = @next_stream_positions[stream]? || 0_u64
    evt = EventRecord.new(
      global_position: @next_global_position,
      stream_position: next_stream_position,
      category: category,
      stream_name: stream,
      message_type: message_type,
      metadata: { id: event_id, at: now }.to_json,
      data: event.to_json
    )
    @store << evt
    @next_global_position += 1
    @next_stream_positions[stream] = next_stream_position + 1
    evt
  end

  def read_json(stream = "*", position : UInt64 = 0u64, category = nil)
    out = [] of EventRecord
    last_position = 0_u64
    @store.each do |evt|
      last_position = evt.global_position
      next if evt.global_position < position
      if category.nil?
        next (out << evt) if stream == "*"
        next (out << evt) if stream == evt.stream_name
      else
        next (out << evt) if evt.category == category
      end
    end
    {last_position + 1, out}
  end

  def read(stream = "*", position : UInt64 = 0u64, category = nil)
    next_position, events = read_json(
      stream, position: position, category: category)
    {next_position, events.map do |evt|
      EventObj.new(
        global_position: evt.global_position,
        stream_position: evt.stream_position,
        category: evt.category,
        stream_name: evt.stream_name,
        message_type: evt.message_type,
        metadata: JSON.parse(evt.metadata),
        data: JSON.parse(evt.data)
      )
    end}
  end
end

class Stream
  def initialize(@store : EventStore, @category : String, @entity_id : String)
  end

  def stream_name
    "#{@category}-#{@entity_id}"
  end

  def write(message_type, message)
    @store.write(@category, stream_name, message_type, message)
  end
end
