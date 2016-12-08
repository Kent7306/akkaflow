package com.kent.pub

/**
 * 深度克隆特质
 */
trait DeepCloneable[A] {
    /**
     * 克隆接口
     */
    def deepClone():A
    /**
     * 克隆帮助接口，用于继承
     */
    def deepCloneAssist(e: A): A
}