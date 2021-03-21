class CreateTransactionalEvent < ActiveRecord::Migration[6.0]
  def self.up
    create_table :transactional_events do |t|
      t.string   :topic,      null: false
      t.json     :payload,    null: false
      t.datetime :created_at, default: -> { 'CURRENT_TIMESTAMP' }
    end
  end

  def self.down
    drop_table :transactional_events
  end
end
