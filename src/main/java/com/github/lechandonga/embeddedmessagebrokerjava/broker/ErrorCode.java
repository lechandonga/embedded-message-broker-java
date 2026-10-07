package com.github.lechandonga.embeddedmessagebrokerjava.broker;

/**
 * 可区分的拒绝原因，所有非法/冲突请求都通过该枚举暴露。
 */
public enum ErrorCode {
    /** 参数非法（空主题、空分区键、非法批量大小等） */
    INVALID_ARGUMENT,
    /** 主题内活跃消息数超过容量上限 */
    CAPACITY_EXCEEDED,
    /** 相同去重键的消息已生效过 */
    DUPLICATE_MESSAGE,
    /** 提交位点低于已提交位点（回退） */
    OFFSET_REGRESSION,
    /** 提交位点跳过了尚未确认的消息 */
    OFFSET_GAP,
    /** 消息不存在或已终结 */
    MESSAGE_NOT_FOUND,
    /** 消息未租约给当前消费者或租约已过期 */
    NOT_LEASED,
    /** 死信不存在 */
    DEAD_LETTER_NOT_FOUND
}
