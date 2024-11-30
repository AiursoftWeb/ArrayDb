namespace Aiursoft.ArrayDb.WriteBuffer.Core;

public static class TimeExtensions
{
    public static int CalculateSleepTime(double maxSleepMilliSecondsWhenCold,
        double stopSleepingWhenWriteBufferItemsMoreThan, int writeBufferItemsCount)
    {
        if (stopSleepingWhenWriteBufferItemsMoreThan <= 0)
        {
            throw new ArgumentException("B must be a positive number.");
        }

        if (writeBufferItemsCount > stopSleepingWhenWriteBufferItemsMoreThan)
        {
            return 0;
        }

        var y = maxSleepMilliSecondsWhenCold * (1 - Math.Log(1 + writeBufferItemsCount) /
            Math.Log(1 + stopSleepingWhenWriteBufferItemsMoreThan));
        return (int)y;
    }
}