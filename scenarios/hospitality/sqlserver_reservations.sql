-- Legacy on-prem SQL Server PMS warehouse table being migrated to the lakehouse.
CREATE TABLE [dbo].[Reservation] (
    [ReservationId]  VARCHAR(12)   NOT NULL,
    [GuestId]        VARCHAR(10)   NOT NULL,
    [PropertyCode]   CHAR(6)       NOT NULL,
    [CheckIn]        DATE          NOT NULL,
    [CheckOut]       DATE          NOT NULL,
    [Rooms]          TINYINT       NOT NULL,
    [RoomRate]       MONEY         NOT NULL,
    [Status]         VARCHAR(12)   NOT NULL,
    [CreatedAt]      DATETIME      NOT NULL DEFAULT GETDATE(),
    [GuestEmail]     NVARCHAR(256) NULL,
    CONSTRAINT [PK_Reservation] PRIMARY KEY CLUSTERED ([ReservationId])
);
GO
