(ns riemann.librato
  "Forwards events to Librato Metrics."
  (:require [clojure.string :as string]
            [riemann.service :as service]
            [clojure.tools.logging :as logging])
  (:use [clj-librato.metrics :only [collate annotate connection-manager
                                    update-annotation]]
        clojure.math.numeric-tower)
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]))

(defn safe-name
  "Converts a string into a safe name for Librato's metrics and streams.
  Converts spaces to periods, preserves only A-Za-z0-9.:-_, and cuts to 255
  characters."
  [s]
  (when s
    (let [s (string/replace s " " ".")
          s (string/replace s #"[^-.:_\w]" "")]
      (subs s 0 (min 255 (count s))))))

(defn event->gauge
  "Converts an event to a gauge."
  [event]
  {:name          (safe-name (:service event))
   :source        (safe-name (:host event))
   :value         (:metric event)
   :measure-time  (round (:time event))})

(def event->counter event->gauge)

(defn event->annotation
  "Converts an event to an annotation."
  [event]
  (into {}
        (filter second
              {:name        (safe-name (:service event))
               :title       (string/join
                              " " [(:service event) (:state event)])
               :source      (safe-name (:host event))
               :description (:description event)
               :start-time  (round (:time event))
               :end-time    (when (:end-time event) (round (:end-time event)))}
              )))

(defn librato-send-queue
  "Read at most max-items from queue before sending them to librato."
  [user api-key http-options queue stop-atom max-items]
  (try
    (let [[k v] (.take ^LinkedBlockingQueue queue)
          msgs (loop [msgs {k [v]}
                      n max-items]         ; poll for more messages
                 (if-let [[k v] (and (pos? n) (.poll ^LinkedBlockingQueue queue))]
                   (recur (update-in msgs [k] (fnil conj []) v)
                          (dec n))
                   msgs))]
      (collate user api-key (:gauge msgs) (:counter msgs) http-options))
    (catch Exception e
      (logging/error e "Unexpected exception in librato service"))))

(defn librato-sender
  "Return a librato sender that will read at most max-items from queue
  before sending them to librato.  Will stop execution if stop-atom is true."
  [user api-key http-options queue stop-atom max-items]
  (fn librato-sender []
    (loop []
      (librato-send-queue user api-key http-options queue stop-atom max-items)
      (if-not @stop-atom
        (recur)))))

(defrecord LibratoService
    [user api-key http-options num-threads max-items queue stop-atom]
  service/Service
  (reload! [service core])
  (start! [service]
    (locking service
      (when-not @stop-atom
        (let [a (atom nil)]
          (dotimes [_ num-threads]
            (-> (Thread.
                 (librato-sender user api-key http-options queue a max-items))
                .start))
          (reset! stop-atom a)))))
  (stop! [service]
    (locking service
      (when @stop-atom
        (reset! @stop-atom true)
        (reset! stop-atom nil))))
  (conflict? [service1 service2]
    (instance? LibratoService service2)))

(defn librato-service
  [user api-key http-options num-threads max-items queue fmap]
  (let [stop-atom (atom false)]
    (merge
     (LibratoService.
      user api-key http-options num-threads max-items queue stop-atom)
     fmap)))

(defn librato-metrics
  "Creates a librato metrics adapter. Takes your username and API key, and
  returns a map of streams:

  :gauge
  :counter
  :annotation
  :start-annotation
  :end-annotation

  Gauge and counter submit events as measurements. Annotation creates an
  annotation from the given event; it will have only a start time unless
  :end-time is given. :start-annotation will *start* an annotation; the
  annotation ID for that host and service will be remembered. :end-annotation
  will submit an end-time for the most recent annotation submitted with
  :start-annotation.

  Example:

  (def librato (librato-metrics \"aphyr@aphyr.com\" \"abcd01234...\"))

  (tagged \"latency\"
    (fixed-event-window 50 (librato :gauge)))

  (where (service \"www\")
    (changed-state
      (where (state \"ok\")
        (:start-annotation librato)
        (else
          (:end-annotation librato)))))

  The option map accepts the following keys:

  :threads     the number of threads to use for sending
  :queue-size  the size of the send queue (in number of events)
  :max-items   the max number of events to send in one request."
  ([user api-key]
     (librato-metrics user api-key {:threads 4 :queue-size 1000}))
  ([user api-key connection-mgr-options]
     (let [annotation-ids (atom {})
           num-threads (:threads connection-mgr-options 4)
           queue-size (:queue-size connection-mgr-options 1000)
           max-items (:max-items connection-mgr-options
                                 (/ queue-size num-threads))
           http-options {:connection-manager
                         (connection-manager connection-mgr-options)}
           queue (LinkedBlockingQueue. queue-size)
           fmap {::http-options http-options
                 :gauge      (fn [& args]
                               (let [data (first args)
                                     events (if (vector? data) data [data])
                                     gauges (map event->gauge events)]
                                 (doseq [gauge gauges]
                                   (.put queue [:gauge gauge]))
                                 (last gauges)))

                 :counter    (fn [& args]
                               (let [data (first args)
                                     events (if (vector? data) data [data])
                                     counters (map event->counter events)]
                                 (doseq [counter counters]
                                   (.put queue [:counter counter]))
                                 (last counters)))

                 :annotation (fn [event]
                               (let [a (event->annotation event)]
                                 (annotate user api-key (:name a)
                                           (dissoc a :name)
                                           http-options)))

                 :start-annotation (fn [event]
                                     (let [a (event->annotation event)
                                           res (annotate user api-key (:name a)
                                                         (dissoc a :name)
                                                         http-options)]
                                       (swap! annotation-ids assoc
                                              [(:host event) (:service event)]
                                              (:id res))
                                       res))

                 :end-annotation (fn [event]
                                   (let [id ((deref annotation-ids)
                                             [(:host event) (:service event)])
                                         a (event->annotation event)]
                                     (when id
                                       (let [res (update-annotation
                                                  user api-key (:name a) id
                                                  {:end-time (round
                                                              (:time event))}
                                                  http-options)]
                                         (swap! annotation-ids dissoc
                                                [(:host event)
                                                 (:service event)])
                                         res))))}]
       (librato-service
        user api-key http-options
        num-threads max-items
        queue fmap))))
