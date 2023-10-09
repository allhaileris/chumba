#import <Foundation/Foundation.h>

@interface Env : NSObject
{
  @private
    NSMutableDictionary *_environment;
  @protected
  @public
}

- (instancetype)initWithFile:(NSString *)path;
- (NSString *)getParameter:(NSString *)key;

@end
