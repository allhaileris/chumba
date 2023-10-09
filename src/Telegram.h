#import <Foundation/Foundation.h>
#import "td_json_client.h"

@interface Telegram : NSThread {
 @private
  void *client;
  int is_closed;
  NSString *_token;
  NSString *_appId;
  NSString *_appHash;
 @protected
 @public
}

- (instancetype)initWithToken:(NSString *)token andAppId:(NSString *)appId andAppHash:(NSString *)appHash;

@end
