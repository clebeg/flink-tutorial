package bigdata.clebeg.cn.model;

public class UserBehavior {
    public long userId;
    public long itemId;
    public long categoryId;
    // 事件ID: (‘pv’, ‘buy’, ‘cart’, ‘fav’)
    public String eventId;
    // 事件发生的时间戳: 单位s
    public long eventTime;

    public UserBehavior() {
    }

    public UserBehavior(long userId, long itemId, long categoryId, String eventId, long eventTime) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.eventId = eventId;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", eventId='" + eventId + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    public static UserBehavior apply(long userId, long itemId, long categoryId, String eventId, long eventTime) {
        return new UserBehavior(userId, itemId, categoryId, eventId, eventTime);
    }
}
