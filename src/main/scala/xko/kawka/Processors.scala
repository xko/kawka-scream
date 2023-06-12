package xko.kawka

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.processor.api.{ContextualProcessor, Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import xko.kawka.FollowerMaze.N

import java.util.Collections

object Processors {
    trait StoreProvider[SK,SV,P <: WithStore[_,_,_,_,SK,SV] ] {
        def provide(name:String):StoreBuilder[KeyValueStore[SK, SV]]
    }

    abstract class WithStore[K,V,RK,RV,SK,SV](storeName: String = "store-"+java.util.UUID.randomUUID.toString) extends ContextualProcessor[K,V,RK,RV] {
        protected var store: KeyValueStore[SK,SV] = _;

        override def init(context: ProcessorContext[RK, RV]): Unit = {
            super.init(context)
            store = context.getStateStore(storeName)
        }
    }

    def memStore[SK: Serde, SV: Serde](name: String): StoreBuilder[KeyValueStore[SK, SV]] =
        Stores.keyValueStoreBuilder[SK, SV](Stores.inMemoryKeyValueStore(name), implicitly[Serde[SK]], implicitly[Serde[SV]])

    def supplier[K, V, RK, RV, SK,SV, P <: WithStore[K,V,RK,RV,SK,SV] ]
                (proc: String => P, storeName: String = "store-"+java.util.UUID.randomUUID.toString)(implicit sm:StoreProvider[SK,SV,P]) = {
        new ProcessorSupplier[K, V, RK, RV] {
            override def get(): Processor[K, V, RK, RV] = proc(storeName)
            override def stores(): java.util.Set[StoreBuilder[_]] = Collections.singleton(implicitly[StoreProvider[SK,SV,P]].provide(storeName))
        }
    }
}
